from functools import wraps
from typing import (
    Any,
    Optional,
    Awaitable,
    Callable,
    Literal,
    Protocol,
    TypeVar,
    overload,
    cast,
)
from collections.abc import Mapping

from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import AsyncRowFactory, dict_row

from .types import TableName, Column, IndexName, c
from . import gen
from ._fixed_execute_utils import has_sql_args, row_to_dict, rows_to_dicts
from ._pool_utils import is_closing_runtime_error
from ._row_utils import get_only_default_from_row
from ._typing import (
    Concatenate,
    Description,
    P,
    R,
    RowDict,
    RowValue,
    Rows,
    SQLArgs,
)
from .errors import QueryResultError


class PGConnectorError(Exception):
    """Custom exception for Postgres Connector errors."""
    pass


class PGConnector:
    """Manages the async connection pool for PostgreSQL."""
    config: Mapping[str, object]
    pool: Optional[AsyncConnectionPool]

    def __init__(self, config: Mapping[str, object]):
        self.config = config
        self.pool = None

    def get(self) -> AsyncConnectionPool:
        """Returns the active connection pool or raises an error."""
        if not self.pool:
            raise PGConnectorError('Not connected to database')
        return self.pool

    async def connect(self) -> bool:
        """Initializes and opens the connection pool."""
        # Close existing pool if it exists
        try:
            if self.pool:
                await self.pool.close()
        except Exception:
            pass

        # Create new pool
        kwargs = {'autocommit': True}
        dsn = cast(str, self.config['dsn'])
        self.pool = AsyncConnectionPool(
            dsn,
            kwargs=kwargs,
            open=False,
        )
        await self.pool.open()

        return True


# Global singleton connector instance
_connector: Optional[PGConnector] = None
# List of callbacks to run upon successful connection
_connected_events: list[Callable[[], Awaitable[object]]] = []


def get_connector() -> PGConnector:
    """Retrieves the global connector instance."""
    if not _connector:
        raise PGConnectorError('Not connected')
    return _connector


def on_connected(func: Callable[[], Awaitable[object]]) -> None:
    """Registers a callback function to be awaited after connection."""
    _connected_events.append(func)


async def connect(config: Mapping[str, object]) -> bool:
    """Initializes the global connector and triggers events."""
    global _connector
    _connector = PGConnector(config)

    if await _connector.connect():
        for evt in _connected_events:
            await evt()
        return True

    return False


async def close() -> None:
    """Closes the global connection pool."""
    if not _connector:
        return
    pool = _connector.get()
    await pool.close()


FixedExecuteReturn = AsyncCursor | RowValue | RowDict | Rows | list[
    RowDict] | None

R_co = TypeVar("R_co", covariant=True)


class RunWithPoolWrappedFunc(Protocol[P, R_co]):

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R_co]:
        ...

    @overload
    def __call__(
        self,
        *args: Any,
        cur: AsyncCursor | None = None,
        **kwargs: Any,
    ) -> Awaitable[R_co]:
        ...


def run_with_pool(
    row_factory_fn: AsyncRowFactory[Any] | None = None,
) -> Callable[
    [Callable[Concatenate[AsyncCursor, P], Awaitable[R]]],
        RunWithPoolWrappedFunc[P, R],
]:
    """
    Decorator to inject a cursor into the function.
    If 'cur' is passed, it uses it. Otherwise, it acquires a new connection
    from the pool, creates a cursor, and handles retries on connection loss.
    """

    def decorator(
        f: Callable[Concatenate[AsyncCursor, P], Awaitable[R]],
    ) -> RunWithPoolWrappedFunc[P, R]:

        @wraps(f)
        async def run(
            *args: Any,
            cur: AsyncCursor | None = None,
            **kwargs: Any,
        ) -> R:
            if _connector is None:
                raise PGConnectorError('Not connected')

            try:
                # If no cursor provided, create a new connection context
                if cur is None:
                    pool = _connector.get()
                    async with pool.connection() as conn:
                        if row_factory_fn is None:
                            async with conn.cursor() as c0:
                                return await f(c0, *args, **kwargs)
                        async with conn.cursor(
                                row_factory=row_factory_fn) as c0:
                            return await f(c0, *args, **kwargs)
                else:
                    # Use the provided cursor
                    return await f(cur, *args, **kwargs)
            except RuntimeError as e:
                # If cursor was provided externally, propagate the error
                if cur is not None:
                    raise e

                # Retry logic for closed connections
                if not is_closing_runtime_error(e):
                    raise e
                connected = await _connector.connect()
                if connected:
                    return await run(*args, cur=None, **kwargs)
                raise e

        return cast(RunWithPoolWrappedFunc[P, R], run)

    return decorator


@overload
async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal[''] = '',
    as_dict: bool = False,
) -> AsyncCursor:
    ...


@overload
async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[False] = False,
) -> RowValue | None:
    ...


@overload
async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[True] = True,
) -> RowDict | None:
    ...


@overload
async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[False] = False,
) -> Rows:
    ...


@overload
async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[True] = True,
) -> list[RowDict]:
    ...


async def fixed_execute(
    cur: AsyncCursor,
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['', 'one', 'all'] = '',
    as_dict: bool = False,
) -> FixedExecuteReturn:
    """Execute SQL; optionally fetch one/all rows and map rows to dict."""
    if has_sql_args(args):
        await cur.execute(sql, args)
    else:
        await cur.execute(sql)
    if fetch == 'one':
        ret = await cur.fetchone()
        if not ret:
            return None
        if not as_dict:
            return ret
        return row_to_dict(ret, cast(Description, cur.description))
    if fetch == 'all':
        rows = await cur.fetchall()
        if not as_dict:
            return rows
        return rows_to_dicts(rows, cast(Description, cur.description))
    if fetch:
        raise ValueError(f'unsupported fetch mode: {fetch}')
    return cur


@run_with_pool()
async def create_table(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes a CREATE TABLE statement."""
    await fixed_execute(cur, gen.gen_create_table(table_name, columns))


@run_with_pool()
async def add_table_column(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes an ALTER TABLE ADD COLUMN statement."""
    await fixed_execute(cur, gen.gen_add_table_column(table_name, columns))


@run_with_pool()
async def create_index(
    cur: AsyncCursor,
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: list[Column],
) -> None:
    """Executes a CREATE INDEX statement."""
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    await fixed_execute(cur, sql)


async def get_only_default(
    cur: AsyncCursor,
    default: object,
    key: Optional[str] = None,
) -> object:
    """Fetches a single value or returns a default."""
    ret = await cur.fetchone()
    return get_only_default_from_row(ret, default, key)


@run_with_pool()
async def insert(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
    args: SQLArgs,
    ret_column: Optional[Column] = None,
    ret_def: Optional[object] = None,
    required: bool = False,
    err_msg: str = 'insert failed',
) -> object | None:
    """Executes an INSERT statement and optionally returns a value."""
    sql = gen.gen_insert(
        table_name=table_name,
        columns=columns,
        ret_column=ret_column,
    )
    await fixed_execute(cur, sql, args)

    if ret_column:
        ret = await get_only_default(cur, ret_def)
        if required and ret is None:
            raise QueryResultError(err_msg)
        return ret
    return None


@run_with_pool()
async def insert_or_update(
        cur: AsyncCursor,
        table_name: TableName,
        uniq_columns: list[Column],
        value_columns: Optional[list[Column]] = None,
        other_columns: Optional[list[Column]] = None,
        args: SQLArgs = (),
) -> None:
    """Executes an INSERT ON CONFLICT DO UPDATE statement."""
    sql = gen.gen_insert_or_update(
        table_name=table_name,
        uniq_columns=uniq_columns,
        value_columns=value_columns,
        other_columns=other_columns,
    )
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def update(
        cur: AsyncCursor,
        table_name: TableName,
        columns: list[Column],
        part_sql: str = '',
        args: SQLArgs = (),
) -> None:
    """Executes an UPDATE statement."""
    sql = gen.gen_update(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
    )
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def delete(
        cur: AsyncCursor,
        table_name: TableName,
        part_sql: str = '',
        args: SQLArgs = (),
) -> None:
    """Executes a DELETE statement."""
    sql = gen.gen_delete(table_name=table_name, part_sql=part_sql)
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def sum(
        cur: AsyncCursor,
        table_name: TableName,
        part_sql: str = '',
        args: SQLArgs = (),
        column: Column = c('*'),
        join_sql: str = '',
) -> object:
    """Executes a SELECT SUM query."""
    sql = gen.gen_sum(
        table_name=table_name,
        part_sql=part_sql,
        column=column,
        join_sql=join_sql,
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


@run_with_pool()
async def count(
    cur: AsyncCursor,
    table_name: TableName,
    part_sql: str = '',
    args: SQLArgs = (),
    column: Column = c('*'),
    join_sql: str = '',
    groups: Optional[str] = None,
) -> object:
    """Executes a SELECT COUNT query."""
    sql = gen.gen_count(
        table_name=table_name,
        part_sql=part_sql,
        column=column,
        join_sql=join_sql,
        groups=groups,
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


@run_with_pool(row_factory_fn=dict_row)
async def select(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
    part_sql: str = '',
    args: SQLArgs = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
    lock_sql: str = '',
    one: bool = False,
    required: bool = False,
    err_msg: str = 'select result is empty',
) -> list[RowDict] | RowDict | None:
    """Executes a SELECT query and returns a list of dictionaries."""
    if one and size is None:
        size = 1
    sql = gen.gen_select(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
        offset=offset,
        size=size,
        groups=groups,
        sorts=sorts,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )
    rows = cast(
        list[RowDict],
        await fixed_execute(cur, sql, args, fetch='all', as_dict=True),
    )
    if one:
        first = rows[0] if rows else None
        if required and not first:
            raise QueryResultError(err_msg)
        return first
    if required and not rows:
        raise QueryResultError(err_msg)
    return rows


async def select_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    args: SQLArgs = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
    lock_sql: str = '',
) -> list[object]:
    """Wraps select to return a list of single values (e.g. list of IDs)."""
    ret = cast(
        list[RowDict],
        await select(
            table_name=table_name,
            columns=[column],
            part_sql=part_sql,
            args=args,
            offset=offset,
            size=size,
            groups=groups,
            sorts=sorts,
            join_sql=join_sql,
            lock_sql=lock_sql,
        ),
    )
    return [list(x.values())[0] for x in ret]


@run_with_pool(row_factory_fn=dict_row)
async def select_one(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
    part_sql: str = '',
    args: SQLArgs = (),
    join_sql: str = '',
    lock_sql: str = '',
) -> Optional[RowDict]:
    """Executes a SELECT query with LIMIT 1."""
    sql = gen.gen_select_one(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )
    return cast(
        Optional[RowDict],
        await fixed_execute(cur, sql, args, fetch='one', as_dict=True),
    )


async def select_one_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    args: SQLArgs = (),
    join_sql: str = '',
    lock_sql: str = '',
) -> object | None:
    """Wraps select_one to return a single scalar value."""
    ret = cast(
        Optional[RowDict],
        await select_one(
            table_name=table_name,
            columns=[column],
            part_sql=part_sql,
            args=args,
            join_sql=join_sql,
            lock_sql=lock_sql,
        ),
    )
    if ret:
        return list(ret.values())[0]
    return None


@run_with_pool()
async def drop_table(cur: AsyncCursor, table_name: TableName) -> None:
    """Executes a DROP TABLE statement."""
    sql = gen.gen_drop_table(table_name)
    await fixed_execute(cur, sql)


@run_with_pool()
async def group_count(
    cur: AsyncCursor,
    table_name: TableName,
    columns: list[Column],
    part_sql: str = '',
    args: SQLArgs = (),
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> object:
    """Executes a COUNT on a grouped subquery."""
    sql = gen.gen_group_count(
        table_name,
        columns=columns,
        part_sql=part_sql,
        groups=groups,
        sorts=sorts,
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)
