from psycopg import AsyncCursor
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from functools import wraps
from typing import Optional, List, Dict, Any, Callable, Coroutine
from mypy_extensions import KwArg, VarArg

from .types import TableName, LeftJoin, Column, IndexName, \
    t, c, c_all, cs, cs_all, i, \
    get_table_name, columns_to_string, get_index_name, \
    constraint_primary_key # noqa

from . import gen_sql as gen


class PGConnnectorError(Exception):
    pass


class PGConnnector():
    config: Dict[str, Any]
    pool: AsyncConnectionPool | None

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None

    def get(self) -> AsyncConnectionPool:
        if not self.pool:
            raise PGConnnectorError('no connected')
        return self.pool

    async def connect(self) -> bool:
        try:
            if self.pool:
                await self.pool.close()
        except Exception:
            pass

        kwargs = {'autocommit': True}
        self.pool = AsyncConnectionPool(self.config['dsn'], kwargs=kwargs, open=False,)
        await self.pool.open()

        return True


_connector: PGConnnector | None = None


def get_connector() -> PGConnnector:
    if not _connector:
        raise PGConnnectorError('not connected')
    return _connector


_connected_events: List[Callable[[], Coroutine[Any, Any, Any]]] = []


def on_connected(func: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
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
    await pool.close()


def run_with_pool(
    row_factory: Any = None
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
                    pool = _connector.get()
                    async with pool.connection() as conn:
                        async with conn.cursor(row_factory=row_factory) as c0:
                            return await f(c0, *args, **kwargs)
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
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    await fixed_execute(cur, gen.gen_create_table(table_name, columns))


@run_with_pool()
async def add_table_column(
    cur: AsyncCursor,
    table_name: TableName,
    columns: List[Column],
) -> None:
    await fixed_execute(cur, gen.gen_add_table_column(table_name, columns))


@run_with_pool()
async def create_index(
    cur: AsyncCursor,
    uniq: str,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> None:
    sql = gen.gen_create_index(uniq, table_name, index_name, columns)
    await fixed_execute(cur, sql)


async def get_only_default(cur: AsyncCursor, default: Any) -> Any:
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

    await fixed_execute(cur, sql, args)


@run_with_pool()
async def update(
        cur: AsyncCursor,
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
    await fixed_execute(cur, sql, args)


@run_with_pool()
async def delete(
        cur: AsyncCursor,
        table_name: TableName,
        part_sql: str = '',
        args: Any = (),
) -> None:
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


@run_with_pool(row_factory=dict_row)
async def select_one(
        cur: AsyncCursor,
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
async def drop_table(cur: AsyncCursor, table_name: TableName) -> None:
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
    sql = gen.gen_group_count(
        table_name,
        columns=columns,
        part_sql=part_sql,
        groups=groups,
        sorts=sorts,
    )
    await fixed_execute(cur, sql, args)
    return await get_only_default(cur, 0)


def fixed_execute(cur: AsyncCursor, sql: str, args: Any = None) -> Any:
    if args and len(args) > 0:
        return cur.execute(sql, args)
    else:
        return cur.execute(sql)
