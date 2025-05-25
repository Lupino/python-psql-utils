from psycopg import Cursor
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from functools import wraps
from typing import Optional, List, Dict, Any, Callable
from mypy_extensions import KwArg, VarArg

from .types import TableName, Column, IndexName, c

from . import gen_sql as gen


class PGConnnectorError(Exception):
    pass


class PGConnnector():
    config: Dict[str, Any]
    pool: ConnectionPool | None

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None

    def get(self) -> ConnectionPool:
        if not self.pool:
            raise PGConnnectorError('no connected')
        return self.pool

    def connect(self) -> bool:
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


_connector: PGConnnector | None = None


def get_connector() -> PGConnnector:
    if not _connector:
        raise PGConnnectorError('not connected')
    return _connector


_connected_events: List[Callable[[], Any]] = []


def on_connected(func: Callable[[], Any]) -> Any:
    _connected_events.append(func)


def connect(config: Any) -> bool:
    global _connector
    _connector = PGConnnector(config)

    if _connector.connect():
        for evt in _connected_events:
            evt()
        return True

    return False


def close() -> None:
    if not _connector:
        return
    pool = _connector.get()
    pool.close()


def run_with_pool(
    row_factory: Any = None
) -> Callable[
    [
        Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Any,
        ],
    ],
        Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Any,
        ],
]:

    def decorator(
        f: Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Any,
        ]
    ) -> Callable[
        [
            VarArg(Any),
            KwArg(Any),
        ],
            Any,
    ]:

        @wraps(f)
        def run(*args: Any, cur: Any = None, **kwargs: Any) -> Any:
            if _connector is None:
                raise PGConnnectorError('not connected')

            try:
                if cur is None:
                    pool = _connector.get()
                    with pool.connection() as conn:
                        with conn.cursor(row_factory=row_factory) as c0:
                            return f(c0, *args, **kwargs)
                else:
                    return f(cur, *args, **kwargs)
            except RuntimeError as e:
                if cur:
                    raise e

                err = str(e)
                if err.find('closing') > -1:
                    connected = _connector.connect()
                    if connected:
                        return run(*args, **kwargs)
                    else:
                        raise e
                else:
                    raise e

        return run

    return decorator


@run_with_pool()
def create_table(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    fixed_execute(cur, gen.gen_create_table(table_name, columns))


@run_with_pool()
def add_table_column(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    fixed_execute(cur, gen.gen_add_table_column(table_name, columns))


@run_with_pool()
def create_index(
    cur: Cursor,
    uniq: str,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> None:
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    fixed_execute(cur, sql)


def get_only_default(cur: Cursor, default: Any) -> Any:
    ret = cur.fetchone()
    if ret is None:
        return default

    return ret[0]


@run_with_pool()
def insert(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
    args: Any,
    ret_column: Optional[Column] = None,
    ret_def: Optional[Any] = None,
) -> Any:
    sql = gen.gen_insert(
        table_name=table_name,
        columns=columns,
        ret_column=ret_column,
    )
    fixed_execute(cur, sql, args)

    if ret_column:
        return get_only_default(cur, ret_def)


@run_with_pool()
def insert_or_update(
        cur: Cursor,
        table_name: TableName,
        uniq_columns: List[Column],
        value_columns: List[Column] = [],
        other_columns: List[Column] = [],
        args: Any = (),
) -> Any:
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
) -> Any:
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
    fixed_execute(cur, sql, args)
    ret = cur.fetchall()
    return [dict(x) for x in ret]


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
) -> Any:
    ret = select(
        table_name,
        [column],
        part_sql,
        args,
        offset,
        size,
        groups,
        sorts,
        join_sql,
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
) -> Any:
    sql = gen.gen_select_one(
        table_name=table_name,
        columns=columns,
        part_sql=part_sql,
        join_sql=join_sql,
    )
    fixed_execute(cur, sql, args)
    ret = cur.fetchone()
    if ret:
        return dict(ret)
    return None


def select_one_only(
        table_name: TableName,
        column: Column,
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
) -> Any:
    ret = select_one(table_name, [column], part_sql, args, join_sql)
    if ret:
        return list(ret.values())[0]

    return None


@run_with_pool()
def drop_table(cur: Cursor, table_name: TableName) -> None:
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
    sql = gen.gen_group_count(
        table_name,
        columns=columns,
        part_sql=part_sql,
        groups=groups,
        sorts=sorts,
    )
    fixed_execute(cur, sql, args)
    return get_only_default(cur, 0)


def fixed_execute(cur: Cursor, sql: str, args: Any = None) -> Any:
    if args and len(args) > 0:
        return cur.execute(sql, args)
    else:
        return cur.execute(sql)
