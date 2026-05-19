from functools import wraps
from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator, Mapping
from typing import (
    Any,
    Callable,
    Optional,
    Literal,
    Protocol,
    SupportsInt,
    TypeVar,
    overload,
    cast,
)

from psycopg import Cursor
from psycopg_pool import ConnectionPool
from psycopg.rows import RowFactory, dict_row

from .types import TableName, Column, IndexName, c, columns_to_string
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
    """Manages the synchronous connection pool for PostgreSQL."""
    config: Mapping[str, object]
    pool: Optional[ConnectionPool]

    def __init__(self, config: Mapping[str, object]):
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
        dsn = cast(str, self.config['dsn'])
        self.pool = ConnectionPool(
            dsn,
            kwargs=kwargs,
            open=False,
        )
        self.pool.open()

        return True


# Global singleton connector instance
_connector: Optional[PGConnector] = None
# List of callbacks to run upon successful connection
_connected_events: list[Callable[[], object]] = []
_current_cursor: ContextVar[Cursor | None] = ContextVar(
    "psql_utils_sync_current_cursor",
    default=None,
)


def get_connector() -> PGConnector:
    """Retrieves the global connector instance."""
    if not _connector:
        raise PGConnectorError('Not connected')
    return _connector


def on_connected(func: Callable[[], object]) -> None:
    """Registers a callback function to run after connection."""
    _connected_events.append(func)


def connect(config: Mapping[str, object]) -> bool:
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


@contextmanager
def with_cursor(cur: Cursor) -> Iterator[Cursor]:
    """Set the current cursor for nested run_with_pool() calls."""
    token = _current_cursor.set(cur)
    try:
        yield cur
    finally:
        _current_cursor.reset(token)


def get_cursor() -> Cursor | None:
    """Returns the scoped cursor set by with_cursor(), if any."""
    return _current_cursor.get()


def _get_cursor_or_raise() -> Cursor:
    """Returns the current scoped cursor or raises when unavailable."""
    cur = get_cursor()
    if cur is None:
        raise PGConnectorError('No active cursor in context')
    return cur


R_co = TypeVar("R_co", covariant=True)


class RunWithPoolWrappedFunc(Protocol[P, R_co]):

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R_co:
        ...


def run_with_pool(
    row_factory_fn: RowFactory[Any] | None = None,
    transaction: bool = False,
) -> Callable[
    [Callable[P, R]],
        RunWithPoolWrappedFunc[P, R],
]:
    """
    Decorator to inject a cursor into the function.
    It first tries a cursor from with_cursor(...). If unavailable, it acquires
    a new connection from the pool, creates a cursor, and handles retries on
    connection loss.
    """

    def decorator(f: Callable[P, R]) -> RunWithPoolWrappedFunc[P, R]:

        @wraps(f)
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            has_scoped_cursor = get_cursor() is not None

            def run_wrapped_in_txn_if_needed(cur: Cursor) -> R:
                if not transaction:
                    return f(*args, **kwargs)
                with cur.connection.transaction():
                    return f(*args, **kwargs)

            try:
                if get_cursor() is not None:
                    return f(*args, **kwargs)

                connector = _connector
                if connector is None:
                    raise PGConnectorError('Not connected')

                # No explicit/current cursor; create a new connection context.
                pool = connector.get()
                with pool.connection() as conn:
                    cursor_cm = (conn.cursor() if row_factory_fn is None else
                                 conn.cursor(row_factory=row_factory_fn))
                    with cursor_cm as c0:
                        with with_cursor(c0):
                            return run_wrapped_in_txn_if_needed(c0)
            except RuntimeError as e:
                # If cursor was provided externally, propagate the error
                if has_scoped_cursor:
                    raise e

                # Retry logic for closed connections
                if not is_closing_runtime_error(e):
                    raise e
                connector = _connector
                if connector is None:
                    raise e
                connected = connector.connect()
                if connected:
                    return run(*args, **kwargs)
                raise e

        return cast(RunWithPoolWrappedFunc[P, R], run)

    return decorator


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal[''] = '',
    as_dict: bool = False,
) -> Cursor:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[False] = False,
) -> RowValue | None:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: Literal[True] = True,
) -> Optional[RowDict]:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['one'] = 'one',
    as_dict: bool = False,
) -> RowValue | RowDict | None:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[False] = False,
) -> Rows:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: Literal[True] = True,
) -> list[RowDict]:
    ...


@overload
def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['all'] = 'all',
    as_dict: bool = False,
) -> Rows | list[RowDict]:
    ...


def fixed_execute(
    sql: str,
    args: SQLArgs | None = None,
    fetch: Literal['', 'one', 'all'] = '',
    as_dict: bool = False,
) -> Cursor | RowValue | RowDict | Rows | list[RowDict] | None:
    """Execute SQL; optionally fetch one/all rows and map rows to dict."""
    cur = _get_cursor_or_raise()
    if has_sql_args(args):
        cur.execute(sql, args)
    else:
        cur.execute(sql)
    if fetch == 'one':
        ret = cur.fetchone()
        if not ret:
            return None
        if not as_dict:
            return ret
        return row_to_dict(ret, cast(Description, cur.description))
    if fetch == 'all':
        rows = cur.fetchall()
        if not as_dict:
            return rows
        return rows_to_dicts(rows, cast(Description, cur.description))
    if fetch:
        raise ValueError(f'unsupported fetch mode: {fetch}')
    return cur


@run_with_pool()
def create_table(
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes a CREATE TABLE statement."""
    fixed_execute(gen.gen_create_table(table_name, columns))


@run_with_pool()
def add_table_column(
    table_name: TableName,
    columns: list[Column],
) -> None:
    """Executes an ALTER TABLE ADD COLUMN statement."""
    fixed_execute(gen.gen_add_table_column(table_name, columns))


@run_with_pool()
def create_index(
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: list[Column],
) -> None:
    """Executes a CREATE INDEX statement."""
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    fixed_execute(sql)


def _validate_returning_args(
    ret_column: Optional[Column],
    ret_columns: Optional[list[Column]],
) -> None:
    if ret_column and ret_columns:
        raise ValueError('ret_column and ret_columns are mutually exclusive')


def _append_returning_sql(
    sql: str,
    ret_column: Optional[Column],
    ret_columns: Optional[list[Column]],
) -> str:
    if ret_columns:
        return f'{sql} RETURNING {columns_to_string(ret_columns)}'
    if ret_column:
        return f'{sql} RETURNING {ret_column}'
    return sql


def _is_empty_result_row(row: RowValue | RowDict | None) -> bool:
    if row is None:
        return True
    if isinstance(row, Mapping):
        return len(row) <= 0
    if isinstance(row, (list, tuple)):
        return len(row) <= 0
    return False


def _execute_write_with_returning(
    sql: str,
    args: SQLArgs,
    ret_column: Optional[Column],
    ret_columns: Optional[list[Column]],
    ret_def: Optional[object],
    required: bool,
    err_msg: str,
    as_dict: bool = False,
) -> int | object | RowValue | RowDict | None:
    if not (ret_column or ret_columns):
        cur = cast(Cursor, fixed_execute(sql, args))
        if required and cur.rowcount <= 0:
            raise QueryResultError(err_msg)
        return cur.rowcount
    row = cast(
        RowValue | RowDict | None,
        fixed_execute(
            _append_returning_sql(sql, ret_column, ret_columns),
            args,
            fetch='one',
            as_dict=as_dict,
        ),
    )
    if required and _is_empty_result_row(row):
        raise QueryResultError(err_msg)
    if ret_columns:
        return row
    ret = get_only_default_from_row(cast(RowValue | None, row), ret_def)
    return ret


@run_with_pool()
def insert(
    table_name: TableName,
    columns: list[Column],
    args: SQLArgs,
    ret_column: Optional[Column] = None,
    ret_columns: Optional[list[Column]] = None,
    ret_def: Optional[object] = None,
    required: bool = False,
    err_msg: str = 'insert failed',
    as_dict: bool = False,
) -> int | object | RowValue | RowDict | None:
    """Executes an INSERT statement; defaults to returning rowcount."""
    _validate_returning_args(ret_column, ret_columns)
    sql = gen.gen_insert(table_name=table_name, columns=columns)
    return _execute_write_with_returning(
        sql=sql,
        args=args,
        ret_column=ret_column,
        ret_columns=ret_columns,
        ret_def=ret_def,
        required=required,
        err_msg=err_msg,
        as_dict=as_dict,
    )


@run_with_pool()
def insert_or_update(
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
    fixed_execute(sql, args)


@run_with_pool()
def update(
    table_name: TableName,
    columns: list[Column],
    part_sql: str = '',
    args: SQLArgs = (),
    ret_column: Optional[Column] = None,
    ret_columns: Optional[list[Column]] = None,
    ret_def: Optional[object] = None,
    required: bool = False,
    err_msg: str = 'update failed',
    as_dict: bool = False,
) -> int | object | RowValue | RowDict | None:
    """Executes an UPDATE statement; defaults to returning rowcount."""
    _validate_returning_args(ret_column, ret_columns)
    sql = gen.gen_update(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
    )
    return _execute_write_with_returning(
        sql=sql,
        args=args,
        ret_column=ret_column,
        ret_columns=ret_columns,
        ret_def=ret_def,
        required=required,
        err_msg=err_msg,
        as_dict=as_dict,
    )


@run_with_pool()
def delete(
        table_name: TableName,
        part_sql: str = '',
        args: SQLArgs = (),
) -> None:
    """Executes a DELETE statement."""
    sql = gen.gen_delete(table_name=table_name, part_sql=part_sql)
    fixed_execute(sql, args)


@run_with_pool()
def sum(
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
    ret = cast(RowValue | None, fixed_execute(sql, args, fetch='one'))
    return int(cast(SupportsInt, get_only_default_from_row(ret, 0)))


@run_with_pool()
def count(
    table_name: TableName,
    part_sql: str = '',
    args: SQLArgs = (),
    column: Column = c('*'),
    join_sql: str = '',
    groups: Optional[str] = None,
) -> int:
    """Executes a SELECT COUNT query."""
    sql = gen.gen_count(
        table_name=table_name,
        part_sql=part_sql,
        column=column,
        join_sql=join_sql,
        groups=groups,
    )
    ret = cast(RowValue | None, fixed_execute(sql, args, fetch='one'))
    return int(cast(SupportsInt, get_only_default_from_row(ret, 0)))


@run_with_pool(row_factory_fn=dict_row)
def select(
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
        fixed_execute(sql, args, fetch='all', as_dict=True),
    )
    if required and not rows:
        raise QueryResultError(err_msg)
    return rows


def select_only(
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
        select(
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
def select_one(
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
        fixed_execute(sql, args, fetch='one', as_dict=True),
    )


def select_one_only(
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
        select_one(
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
def drop_table(table_name: TableName) -> None:
    """Executes a DROP TABLE statement."""
    sql = gen.gen_drop_table(table_name)
    fixed_execute(sql)


@run_with_pool()
def group_count(
    table_name: TableName,
    columns: list[Column],
    part_sql: str = '',
    args: SQLArgs = (),
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> int:
    """Executes a COUNT on a grouped subquery."""
    sql = gen.gen_group_count(
        table_name,
        columns=columns,
        part_sql=part_sql,
        groups=groups,
        sorts=sorts,
    )
    ret = cast(RowValue | None, fixed_execute(sql, args, fetch='one'))
    return int(cast(SupportsInt, get_only_default_from_row(ret, 0)))
