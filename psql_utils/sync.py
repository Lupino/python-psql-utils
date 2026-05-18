from functools import wraps
from typing import Optional, List, Dict, Any, Callable, cast

from psycopg import Cursor
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from .types import TableName, Column, IndexName, c
from . import gen


class PGConnectorError(Exception):
    """Custom exception for Postgres Connector errors."""
    pass


class PGConnector:
    """Manages the synchronous connection pool for PostgreSQL."""
    config: Dict[str, Any]
    pool: Optional[ConnectionPool]

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None

    def get(self) -> ConnectionPool:
        """Returns the active connection pool or raises an error."""
        if not self.pool:
            raise PGConnectorError('Not connected to database')
        return self.pool

    def connect(self) -> bool:
        """Initializes and opens the connection pool."""
        try:
            if self.pool:
                self.pool.close()
        except Exception:
            pass

        kwargs = {'autocommit': True}
        self.pool = ConnectionPool(
            self.config['dsn'],
            kwargs=kwargs,
            open=False,
        )
        self.pool.open()

        return True


# Global singleton connector instance
_connector: Optional[PGConnector] = None
# List of callbacks to run upon successful connection
_connected_events: List[Callable[[], Any]] = []


def get_connector() -> PGConnector:
    """Retrieves the global connector instance."""
    if not _connector:
        raise PGConnectorError('Not connected')
    return _connector


def on_connected(func: Callable[[], Any]) -> None:
    """Registers a callback function to run after connection."""
    _connected_events.append(func)


def connect(config: Any) -> bool:
    """Initializes the global connector and triggers events."""
    global _connector
    _connector = PGConnector(config)

    if _connector.connect():
        for evt in _connected_events:
            evt()
        return True

    return False


def close() -> None:
    """Closes the global connection pool."""
    if not _connector:
        return
    pool = _connector.get()
    pool.close()


def run_with_pool(
    row_factory: Any = None
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to inject a cursor into the function.
    If 'cur' is passed, it uses it. Otherwise, it acquires a new connection
    from the pool, creates a cursor, and handles retries on connection loss.
    """

    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:

        @wraps(f)
        def run(*args: Any, cur: Any = None, **kwargs: Any) -> Any:
            if _connector is None:
                raise PGConnectorError('Not connected')

            try:
                # If no cursor provided, create a new connection context
                if cur is None:
                    pool = _connector.get()
                    with pool.connection() as conn:
                        with conn.cursor(row_factory=row_factory) as c0:
                            return f(c0, *args, **kwargs)
                else:
                    # Use the provided cursor
                    return f(cur, *args, **kwargs)
            except RuntimeError as e:
                # If cursor was provided externally, propagate the error
                if cur:
                    raise e

                # Retry logic for closed connections
                err_msg = str(e)
                if 'closing' in err_msg:
                    connected = _connector.connect()
                    if connected:
                        return run(*args, **kwargs)
                    else:
                        raise e
                else:
                    raise e

        return run

    return decorator


def fixed_execute(
    cur: Cursor,
    sql: str,
    args: Any = None,
    fetch: str = '',
    as_dict: bool = False,
) -> Any:
    """Execute SQL; optionally fetch one/all rows and map rows to dict."""
    if args and len(args) > 0:
        cur.execute(sql, args)
    else:
        cur.execute(sql)
    if fetch == 'one':
        ret = cur.fetchone()
        if not ret:
            return None
        if not as_dict:
            return ret
        ret_any: Any = ret
        if isinstance(ret_any, dict):
            return dict(ret_any)
        cols = [d.name for d in (cur.description or [])]
        return dict(zip(cols, ret))
    if fetch == 'all':
        rows = cur.fetchall()
        if not as_dict:
            return rows
        if not rows:
            return []
        first = rows[0]
        first_any: Any = first
        if isinstance(first_any, dict):
            return [dict(row) for row in rows]
        cols = [d.name for d in (cur.description or [])]
        return [dict(zip(cols, row)) for row in rows]
    return cur


@run_with_pool()
def create_table(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    """Executes a CREATE TABLE statement."""
    fixed_execute(cur, gen.gen_create_table(table_name, columns))


@run_with_pool()
def add_table_column(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    """Executes an ALTER TABLE ADD COLUMN statement."""
    fixed_execute(cur, gen.gen_add_table_column(table_name, columns))


@run_with_pool()
def create_index(
    cur: Cursor,
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> None:
    """Executes a CREATE INDEX statement."""
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    fixed_execute(cur, sql)


def get_only_default(
    cur: Cursor,
    default: Any,
    key: Optional[str] = None,
) -> Any:
    """Fetches a single value or returns a default."""
    ret = cur.fetchone()
    if ret is None:
        return default
    ret_any: Any = ret
    if key and isinstance(ret_any, dict):
        return ret_any.get(key, default)
    return ret[0]


@run_with_pool()
def insert(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
    args: Any,
    ret_column: Optional[Column] = None,
    ret_def: Optional[Any] = None,
    required: bool = False,
    err_msg: str = 'insert failed',
) -> Any:
    """Executes an INSERT statement and optionally returns a value."""
    sql = gen.gen_insert(
        table_name=table_name,
        columns=columns,
        ret_column=ret_column,
    )
    fixed_execute(cur, sql, args)

    if ret_column:
        ret = get_only_default(cur, ret_def)
        if required and ret is None:
            raise Exception(err_msg)
        return ret
    return None


@run_with_pool()
def insert_or_update(
        cur: Cursor,
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
    fixed_execute(cur, sql, args)


@run_with_pool()
def update(
        cur: Cursor,
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
    fixed_execute(cur, sql, args)


@run_with_pool()
def delete(
        cur: Cursor,
        table_name: TableName,
        part_sql: str = '',
        args: Any = (),
) -> None:
    """Executes a DELETE statement."""
    sql = gen.gen_delete(table_name=table_name, part_sql=part_sql)
    fixed_execute(cur, sql, args)


@run_with_pool()
def sum(
        cur: Cursor,
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
    fixed_execute(cur, sql, args)
    return get_only_default(cur, 0)


@run_with_pool()
def count(
    cur: Cursor,
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
    fixed_execute(cur, sql, args)
    return get_only_default(cur, 0)


@run_with_pool(row_factory=dict_row)
def select(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    args: Any = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
    lock_sql: str = '',
    one: bool = False,
    required: bool = False,
    err_msg: str = 'select result is empty',
) -> Any:
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
        List[Dict[str, Any]],
        fixed_execute(cur, sql, args, fetch='all', as_dict=True),
    )
    if one:
        first = rows[0] if rows else None
        if required and not first:
            raise Exception(err_msg)
        return first
    if required and not rows:
        raise Exception(err_msg)
    return rows


def select_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    args: Any = (),
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
    lock_sql: str = '',
) -> List[Any]:
    """Wraps select to return a list of single values (e.g. list of IDs)."""
    ret = select(
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
    )
    return [list(x.values())[0] for x in ret]


@run_with_pool(row_factory=dict_row)
def select_one(
        cur: Cursor,
        table_name: TableName,
        columns: List[Column],
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
        lock_sql: str = '',
) -> Optional[Dict[str, Any]]:
    """Executes a SELECT query with LIMIT 1."""
    sql = gen.gen_select_one(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )
    return cast(
        Optional[Dict[str, Any]],
        fixed_execute(cur, sql, args, fetch='one', as_dict=True),
    )


def select_one_only(
        table_name: TableName,
        column: Column,
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
        lock_sql: str = '',
) -> Any:
    """Wraps select_one to return a single scalar value."""
    ret = select_one(
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        args=args,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )
    if ret:
        return list(ret.values())[0]
    return None


@run_with_pool()
def drop_table(cur: Cursor, table_name: TableName) -> None:
    """Executes a DROP TABLE statement."""
    sql = gen.gen_drop_table(table_name)
    fixed_execute(cur, sql)


@run_with_pool()
def group_count(
    cur: Cursor,
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
    fixed_execute(cur, sql, args)
    return get_only_default(cur, 0)
