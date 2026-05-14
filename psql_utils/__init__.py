from functools import wraps
from typing import Optional, List, Dict, Any, Callable, Coroutine

from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from .types import TableName, Column, IndexName, c
from . import gen


class PGConnectorError(Exception):
    """Custom exception for Postgres Connector errors."""
    pass


class PGConnector:
    """Manages the async connection pool for PostgreSQL."""
    config: Dict[str, Any]
    pool: Optional[AsyncConnectionPool]

    def __init__(self, config: Dict[str, Any]):
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
        self.pool = AsyncConnectionPool(
            self.config['dsn'],
            kwargs=kwargs,
            open=False,
        )
        await self.pool.open()

        return True


# Global singleton connector instance
_connector: Optional[PGConnector] = None
# List of callbacks to run upon successful connection
_connected_events: List[Callable[[], Coroutine[Any, Any, Any]]] = []


def get_connector() -> PGConnector:
    """Retrieves the global connector instance."""
    if not _connector:
        raise PGConnectorError('Not connected')
    return _connector


def on_connected(func: Callable[[], Coroutine[Any, Any, Any]]) -> None:
    """Registers a callback function to be awaited after connection."""
    _connected_events.append(func)


async def connect(config: Any) -> bool:
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


RunWithPoolFunc = Callable[..., Coroutine[Any, Any, Any]]


def run_with_pool(
    row_factory: Any = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to inject a cursor into the function.
    If 'cur' is passed, it uses it. Otherwise, it acquires a new connection
    from the pool, creates a cursor, and handles retries on connection loss.
    """

    def decorator(f: RunWithPoolFunc) -> RunWithPoolFunc:

        @wraps(f)
        async def run(*args: Any, cur: Any = None, **kwargs: Any) -> Any:
            if _connector is None:
                raise PGConnectorError('Not connected')

            try:
                # If no cursor provided, create a new connection context
                if cur is None:
                    pool = _connector.get()
                    async with pool.connection() as conn:
                        async with conn.cursor(row_factory=row_factory) as c0:
                            return await f(c0, *args, **kwargs)
                else:
                    # Use the provided cursor
                    return await f(cur, *args, **kwargs)
            except RuntimeError as e:
                # If cursor was provided externally, propagate the error
                if cur:
                    raise e

                # Retry logic for closed connections
                err_msg = str(e)
                if 'closing' in err_msg:
                    connected = await _connector.connect()
                    if connected:
                        return await run(*args, **kwargs)
                    else:
                        raise e
                else:
                    raise e

        return run

    return decorator


async def fixed_execute(cur: AsyncCursor, sql: str, args: Any = None) -> Any:
    """Helper to execute SQL with optional arguments."""
    if args and len(args) > 0:
        return await cur.execute(sql, args)
    return await cur.execute(sql)


@run_with_pool()
async def create_table(
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    """Executes a CREATE TABLE statement."""
    await fixed_execute(cur, gen.gen_create_table(table_name, columns))


@run_with_pool()
async def add_table_column(
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    """Executes an ALTER TABLE ADD COLUMN statement."""
    await fixed_execute(cur, gen.gen_add_table_column(table_name, columns))


@run_with_pool()
async def create_index(
    cur: AsyncCursor,
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> None:
    """Executes a CREATE INDEX statement."""
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    await fixed_execute(cur, sql)


async def get_only_default(cur: AsyncCursor, default: Any) -> Any:
    """Fetches a single value or returns a default."""
    ret = await cur.fetchone()
    if ret is None:
        return default
    return ret[0]


@run_with_pool()
async def insert(
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
    args: Any,
    ret_column: Optional[Column] = None,
    ret_def: Optional[Any] = None,
) -> Any:
    """Executes an INSERT statement and optionally returns a value."""
    sql = gen.gen_insert(
        table_name=table_name,
        columns=columns,
        ret_column=ret_column,
    )
    await fixed_execute(cur, sql, args)

    if ret_column:
        return await get_only_default(cur, ret_def)


@run_with_pool()
async def insert_or_update(
        cur: AsyncCursor,
        table_name: TableName,
        uniq_columns: List[Column],
        value_columns: Optional[List[Column]] = None,
        other_columns: Optional[List[Column]] = None,
        args: Any = (),
) -> Any:
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
        columns: List[Column],
        part_sql: str = '',
        args: Any = (),
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
        args: Any = (),
) -> None:
    """Executes a DELETE statement."""
    sql = gen.gen_delete(table_name=table_name, part_sql=part_sql)
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def sum(
        cur: AsyncCursor,
        table_name: TableName,
        part_sql: str = '',
        args: Any = (),
        column: Column = c('*'),
        join_sql: str = '',
) -> Any:
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
    args: Any = (),
    column: Column = c('*'),
    join_sql: str = '',
    groups: Optional[str] = None,
) -> Any:
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


@run_with_pool(row_factory=dict_row)
async def select(
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    args: Any = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
) -> List[Dict[str, Any]]:
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
    )
    await fixed_execute(cur, sql, args)
    ret = await cur.fetchall()
    return [dict(x) for x in ret]


async def select_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    args: Any = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
) -> List[Any]:
    """Wraps select to return a list of single values (e.g. list of IDs)."""
    ret = await select(
        cur=None,  # Passed as None to trigger pool acquisition in decorator
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        args=args,
        offset=offset,
        size=size,
        groups=groups,
        sorts=sorts,
        join_sql=join_sql,
    )
    return [list(x.values())[0] for x in ret]


@run_with_pool(row_factory=dict_row)
async def select_one(
        cur: AsyncCursor,
        table_name: TableName,
        columns: List[Column],
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
) -> Optional[Dict[str, Any]]:
    """Executes a SELECT query with LIMIT 1."""
    sql = gen.gen_select_one(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
        join_sql=join_sql,
    )
    await fixed_execute(cur, sql, args)
    ret = await cur.fetchone()
    if ret:
        return dict(ret)
    return None


async def select_one_only(
        table_name: TableName,
        column: Column,
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
) -> Any:
    """Wraps select_one to return a single scalar value."""
    ret = await select_one(
        cur=None,
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        args=args,
        join_sql=join_sql,
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
    columns: List[Column],
    part_sql: str = '',
    args: Any = (),
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> Any:
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
