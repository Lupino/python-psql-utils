import aiopg
from aiopg import Cursor, Pool

from functools import wraps
from psycopg2.extras import DictCursor
from typing import Optional, List, Dict, Any, Callable, Coroutine
from mypy_extensions import KwArg, VarArg


class TableName(object):
    table_name: str
    alias_name: str | None
    joins: List['LeftJoin']

    def __init__(
        self,
        table_name: str,
        alias: Optional[str] = None,
        joins: List['LeftJoin'] = [],
    ) -> None:
        self.table_name = table_name
        self.alias_name = alias
        self.joins = joins

    def alias(self, alias: str) -> 'TableName':
        return TableName(self.table_name, alias, self.joins[:])

    def join(self, table: 'TableName', where: str) -> 'TableName':
        joins = self.joins[:]
        joins.append(LeftJoin(table, where))

        return TableName(self.table_name, self.alias_name, joins)

    def __str__(self) -> str:
        if self.alias_name is None:
            table_name = f'"{self.table_name}"'
        else:
            table_name = f'"{self.table_name}" AS {self.alias_name}'

        if len(self.joins) > 0:
            table_name += ' ' + ' '.join([str(join) for join in self.joins])

        return table_name


class LeftJoin(object):
    table_name: TableName
    where: str

    def __init__(self, table_name: TableName, where: str) -> None:
        self.table_name = table_name
        self.where = where

    def __str__(self) -> str:
        return f'''LEFT JOIN {str(self.table_name)} ON {self.where} '''


def get_table_name(table_name: List[TableName] | TableName) -> str:
    if isinstance(table_name, list):
        return ', '.join([str(tn) for tn in table_name])

    return str(table_name)


def t(table_name: str) -> TableName:
    return TableName(table_name)


class Column(object):
    column: str

    def __init__(self, column: str) -> None:
        self.column = column

    def __str__(self) -> str:
        return self.column


def c(column: str) -> Column:
    return Column(column)


c_all = c('*')


def cs(columns: List[str]) -> List[Column]:
    return [c(x) for x in columns]


cs_all = cs(['*'])


def columns_to_string(columns: List[Column]) -> str:
    return ', '.join([str(x) for x in columns])


class IndexName(object):
    index_name: str

    def __init__(self, index_name: str) -> None:
        self.index_name = index_name

    def __str__(self) -> str:
        return self.index_name


def i(index_name: str) -> IndexName:
    return IndexName(index_name)


def get_index_name(table_name: TableName, index_name: IndexName) -> str:
    return '"{}_{}"'.format(table_name.table_name, index_name.index_name)


def constraint_primary_key(table_name: TableName,
                           columns: List[Column]) -> Column:
    return Column('CONSTRAINT {} PRIMARY KEY ({})'.format(
        get_index_name(table_name, i('pk')), columns_to_string(columns)))


class PGConnnectorError(Exception):
    pass


class PGConnnector():
    config: Dict[str, Any]
    pool: Pool | None

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None

    def get(self) -> Pool:
        if not self.pool:
            raise PGConnnectorError('no connected')
        return self.pool

    async def connect(self) -> bool:
        try:
            if self.pool:
                self.pool.close()
        except Exception:
            pass

        self.pool = await aiopg.create_pool(**self.config)

        return True


_connector: PGConnnector | None = None


def get_connector() -> PGConnnector:
    if not _connector:
        raise PGConnnectorError('not connected')
    return _connector


_connected_events: List[Callable[[], Coroutine[Any, Any, Any]]] = []


def on_connected(func: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    global _connected_events
    _connected_events.append(func)


async def connect(config: Any) -> bool:
    global _connector
    _connector = PGConnnector(config)

    if await _connector.connect():
        for evt in _connected_events:
            await evt()
        return True

    return False


async def close() -> None:
    if not _connector:
        return
    pool = _connector.get()
    pool.close()
    await pool.wait_closed()


def run_with_pool(
    cursor_factory: Any = None
) -> Callable[
    [
        Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Coroutine[Any, Any, Any],
        ],
    ],
        Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Coroutine[Any, Any, Any],
        ],
]:

    def decorator(
        f: Callable[
            [
                VarArg(Any),
                KwArg(Any),
            ],
            Coroutine[Any, Any, Any],
        ]
    ) -> Callable[
        [
            VarArg(Any),
            KwArg(Any),
        ],
            Coroutine[Any, Any, Any],
    ]:

        @wraps(f)
        async def run(*args: Any, cur: Any = None, **kwargs: Any) -> Any:
            if _connector is None:
                raise PGConnnectorError('not connected')

            try:
                if cur is None:
                    async with _connector.get().acquire() as conn:
                        async with conn.cursor(
                                cursor_factory=cursor_factory) as cur0:
                            return await f(cur0, *args, **kwargs)
                else:
                    return await f(cur, *args, **kwargs)
            except RuntimeError as e:
                if cur:
                    raise e

                err = str(e)
                if err.find('closing') > -1:
                    connected = await _connector.connect()
                    if connected:
                        return await run(*args, **kwargs)
                    else:
                        raise e
                else:
                    raise e

        return run

    return decorator


@run_with_pool()
async def create_table(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    await fixed_execute(
        cur, 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
            get_table_name(table_name), columns_to_string(columns)))


@run_with_pool()
async def add_table_column(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    await fixed_execute(
        cur, 'ALTER TABLE {} ADD COLUMN {}'.format(get_table_name(table_name),
                                                   columns_to_string(columns)))


@run_with_pool()
async def create_index(
    cur: Cursor,
    uniq: str,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> None:
    uniq_word = 'UNIQUE ' if uniq else ''
    await fixed_execute(
        cur, 'CREATE {}INDEX IF NOT EXISTS {} ON {} ({})'.format(
            uniq_word, get_index_name(table_name, index_name),
            get_table_name(table_name), columns_to_string(columns)))


async def get_only_default(cur: Cursor, default: Any) -> Any:
    ret = await cur.fetchone()
    if ret is None:
        return default
    if ret[0]:
        return ret[0]
    else:
        return default


@run_with_pool()
async def insert(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
    args: Any,
    ret_column: Optional[Column] = None,
    ret_def: Optional[Any] = None,
) -> Any:
    v = [Column('%s') for x in columns]
    ret_sql = ' returning {}'.format(ret_column) if ret_column else ''
    await fixed_execute(
        cur,
        'INSERT INTO {} ({}) VALUES ({}){}'.format(get_table_name(table_name),
                                                   columns_to_string(columns),
                                                   columns_to_string(v),
                                                   ret_sql), args)

    if ret_column:
        return await get_only_default(cur, ret_def)


def append_excluded_set(column: Column) -> str:
    col = str(column)
    if col.find('=') > -1:
        return col
    return "{} = excluded.{}".format(col, col)


@run_with_pool()
async def insert_or_update(
        cur: Cursor,
        table_name: TableName,
        uniq_columns: List[Column],
        value_columns: List[Column] = [],
        other_columns: List[Column] = [],
        args: Any = (),
) -> Any:
    cols = uniq_columns + value_columns + other_columns
    v = [Column('%s') for x in cols]
    set_sql = ', '.join([append_excluded_set(x) for x in value_columns])
    do_sql = " DO UPDATE SET {}".format(
        set_sql) if value_columns else " DO NOTHING"
    sql = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}".format(
        get_table_name(table_name), columns_to_string(cols),
        columns_to_string(v), columns_to_string(uniq_columns), do_sql)

    await fixed_execute(cur, sql, args)


def append_update_set(column: Column) -> str:
    col = str(column)
    if col.find('=') > -1:
        return col
    return "{} = %s".format(col)


@run_with_pool()
async def update(
        cur: Cursor,
        table_name: TableName,
        columns: List[Column],
        part_sql: str = '',
        args: Any = (),
) -> None:
    set_sql = ', '.join([append_update_set(x) for x in columns])
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    sql = "UPDATE {} SET {}{}".format(get_table_name(table_name), set_sql,
                                      where_sql)
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def delete(
        cur: Cursor,
        table_name: TableName,
        part_sql: str = '',
        args: Any = (),
) -> None:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    sql = 'DELETE FROM {}{}'.format(get_table_name(table_name), where_sql)
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def sum(
        cur: Cursor,
        table_name: TableName,
        part_sql: str = '',
        args: Any = (),
        column: Column = c('*'),
        join_sql: str = '',
) -> Any:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    sql = 'SELECT sum({}) FROM {}{}{}'.format(str(column),
                                              get_table_name(table_name),
                                              join_sql, where_sql)
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


@run_with_pool()
async def count(
    cur: Cursor,
    table_name: TableName,
    part_sql: str = '',
    args: Any = (),
    column: Column = c('*'),
    join_sql: str = '',
    groups: Optional[str] = None,
) -> Any:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    sql = 'SELECT count({}) FROM {}{}{}{}'.format(
        str(column),
        get_table_name(table_name),
        join_sql,
        where_sql,
        format_group_and_sort_sql(groups, None),
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


@run_with_pool(cursor_factory=DictCursor)
async def select(
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
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    limit_sql = '' if size is None else ' LIMIT {}'.format(size)
    offset_sql = '' if offset is None else ' OFFSET {}'.format(offset)
    sql = "SELECT {} FROM {}{}{} {}{}{}".format(
        columns_to_string(columns),
        get_table_name(table_name),
        join_sql,
        where_sql,
        format_group_and_sort_sql(groups, sorts),
        limit_sql,
        offset_sql,
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
) -> Any:
    ret = await select(
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


@run_with_pool(cursor_factory=DictCursor)
async def select_one(
        cur: Cursor,
        table_name: TableName,
        columns: List[Column],
        part_sql: str = '',
        args: Any = (),
        join_sql: str = '',
) -> Any:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    sql = "SELECT {} FROM {}{}{} LIMIT 1".format(columns_to_string(columns),
                                                 get_table_name(table_name),
                                                 join_sql, where_sql)
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
    ret = await select_one(table_name, [column], part_sql, args, join_sql)
    if ret:
        return list(ret.values())[0]

    return None


@run_with_pool()
async def drop_table(cur: Cursor, table_name: TableName) -> None:
    name = get_table_name(table_name)
    await fixed_execute(cur, f'drop table {name}')


def gen_ordering_sql(column: Column, arr: List[Any]) -> tuple[str, str]:
    ret = []
    for ordering, a in enumerate(arr):
        ret.append('({}, {})'.format(a, ordering))

    return 'JOIN (VALUES {}) AS x (id, ordering) ON {} = x.id'.format(
        ', '.join(ret), str(column)), 'ORDER BY x.ordering'


@run_with_pool()
async def group_count(
    cur: Cursor,
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    args: Any = (),
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> Any:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    sql = "SELECT COUNT(*) FROM (SELECT {} FROM {}{}{}) G".format(
        columns_to_string(columns),
        get_table_name(table_name),
        where_sql,
        format_group_and_sort_sql(groups, sorts),
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


def fixed_execute(cur: Cursor, sql: str, args: Any = None) -> Any:
    if args and len(args) > 0:
        return cur.execute(sql, args)
    else:
        return cur.execute(sql)


def format_group_and_sort_sql(groups: str | None, sorts: str | None) -> str:
    group_sql = f' GROUP BY {groups}' if groups else ''
    sort_sql = f' ORDER BY {sorts}' if sorts else ''
    return group_sql + sort_sql
