from functools import wraps
from contextlib import asynccontextmanager
from contextvars import ContextVar
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
from collections.abc import AsyncIterator, Mapping

from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import AsyncRowFactory, dict_row

from .types import TableName, Column, IndexName, c
from . import gen
from ._fixed_execute_utils import has_sql_args, row_to_dict, rows_to_dicts
from ._pool_utils import is_closing_runtime_error
from ._row_utils import get_only_default_from_row
from ._typing import (
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
_current_cursor: ContextVar[AsyncCursor | None] = ContextVar(
    "psql_utils_async_current_cursor",
    default=None,
)


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


@asynccontextmanager
async def with_cursor(cur: AsyncCursor) -> AsyncIterator[AsyncCursor]:
    """Set the current cursor for nested run_with_pool() calls."""
    token = _current_cursor.set(cur)
    try:
        yield cur
    finally:
        _current_cursor.reset(token)


def get_cursor() -> AsyncCursor | None:
    """Returns the scoped cursor set by with_cursor(), if any."""
    return _current_cursor.get()


def _get_cursor_or_raise() -> AsyncCursor:
    """Returns the current scoped cursor or raises when unavailable."""
    cur = get_cursor()
    if cur is None:
        raise PGConnectorError('No active cursor in context')
    return cur


FixedExecuteReturn = AsyncCursor | RowValue | RowDict | Rows | list[
    RowDict] | None

R_co = TypeVar("R_co", covariant=True)


class RunWithPoolWrappedFunc(Protocol[P, R_co]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R_co]:
        ...


def run_with_pool(
    row_factory_fn: AsyncRowFactory[Any] | None = None,
) -> Callable[
    [Callable[P, Awaitable[R]]],
        RunWithPoolWrappedFunc[P, R],
]:
    """
    Decorator to inject a cursor into the function.
    It first tries a cursor from with_cursor(...). If unavailable, it acquires
    a new connection from the pool, creates a cursor, and handles retries on
    connection loss.
    """

    def decorator(
        f: Callable[P, Awaitable[R]],
    ) -> RunWithPoolWrappedFunc[P, R]:

        @wraps(f)
        async def run(*args: P.args, **kwargs: P.kwargs) -> R:
            has_scoped_cursor = get_cursor() is not None

            try:
                if get_cursor() is not None:
                    return await f(*args, **kwargs)

                connector = _connector
                if connector is None:
                    raise PGConnectorError('Not connected')

                # No explicit/current cursor; create a new connection context.
                pool = connector.get()
                async with pool.connection() as conn:
                    if row_factory_fn is None:
                        async with conn.cursor() as c0:
                            async with with_cursor(c0):
                                return await f(*args, **kwargs)
                    async with conn.cursor(
                            row_factory=row_factory_fn) as c0:
                        async with with_cursor(c0):
                            return await f(*args, **kwargs)
            except RuntimeError as e:
                # If cursor was provided externally, propagate the error.
                if has_scoped_cursor:
                    raise e

                # Retry logic for closed connections
                if not is_closing_runtime_error(e):
                    raise e
                connector = _connector
                if connector is None:
                    raise e
                connected = await connector.connect()
                if connected:
                    return await run(*args, **kwargs)
                raise e

        return cast(RunWithPoolWrappedFunc[P, R], run)

    return decorator


@overload
async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal[''] = '',
    as_dict: bool = False,
) -> AsyncCursor:
    ...


@overload
async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[False] = False,
) -> RowValue | None:
    ...


@overload
async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[True] = True,
) -> RowDict | None:
    ...


@overload
async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[False] = False,
) -> Rows:
    ...


@overload
async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[True] = True,
) -> list[RowDict]:
    ...


async def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['', 'one', 'all'] = '',
    as_dict: bool = False,
) -> FixedExecuteReturn:
    """Execute SQL; optionally fetch one/all rows and map rows to dict."""
    cur = _get_cursor_or_raise()
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
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes a CREATE TABLE statement."""
    await fixed_execute(gen.gen_create_table(table_name, columns))


@run_with_pool()
async def add_table_column(
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes an ALTER TABLE ADD COLUMN statement."""
    await fixed_execute(gen.gen_add_table_column(table_name, columns))


@run_with_pool()
async def create_index(
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: list[Column],
) -> None:
    """Executes a CREATE INDEX statement."""
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    await fixed_execute(sql)


async def get_only_default(
    default: object,
    key: Optional[str] = None,
) -> object:
    """Fetches a single value or returns a default."""
    cur = _get_cursor_or_raise()
    ret = await cur.fetchone()
    return get_only_default_from_row(ret, default, key)


@run_with_pool()
async def insert(
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
    await fixed_execute(sql, args)

    if ret_column:
        ret = await get_only_default(ret_def)
        if required and ret is None:
            raise QueryResultError(err_msg)
        return ret
    return None


@run_with_pool()
async def insert_or_update(
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
    await fixed_execute(sql, args)


@run_with_pool()
async def update(
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
    await fixed_execute(sql, args)


@run_with_pool()
async def delete(
        table_name: TableName,
        part_sql: str = '',
        args: SQLArgs = (),
) -> None:
    """Executes a DELETE statement."""
    sql = gen.gen_delete(table_name=table_name, part_sql=part_sql)
    await fixed_execute(sql, args)


@run_with_pool()
async def sum(
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
    await fixed_execute(sql, args)
    return await get_only_default(0)


@run_with_pool()
async def count(
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
    await fixed_execute(sql, args)
    return await get_only_default(0)


@run_with_pool(row_factory_fn=dict_row)
async def select(
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
    required: bool = False,
    err_msg: str = 'select result is empty',
) -> list[RowDict]:
    """Executes a SELECT query and returns a list of dictionaries."""
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
        await fixed_execute(sql, args, fetch='all', as_dict=True),
    )
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
        await fixed_execute(sql, args, fetch='one', as_dict=True),
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
async def drop_table(table_name: TableName) -> None:
    """Executes a DROP TABLE statement."""
    sql = gen.gen_drop_table(table_name)
    await fixed_execute(sql)


@run_with_pool()
async def group_count(
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
    await fixed_execute(sql, args)
    return await get_only_default(0)
