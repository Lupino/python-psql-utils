from typing import Optional, List, Any

from .types import TableName, Column, IndexName, get_table_name, \
    columns_to_string, get_index_name


def gen_create_table(
    table_name: TableName,
    columns: List[Column],
) -> str:
    return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
        get_table_name(table_name),
        columns_to_string(columns),
    )


def gen_add_table_column(
    table_name: TableName,
    columns: List[Column],
) -> str:
    return 'ALTER TABLE {} ADD COLUMN {}'.format(
        get_table_name(table_name),
        columns_to_string(columns),
    )


def gen_create_index(
    uniq: str,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> str:
    uniq_word = 'UNIQUE ' if uniq else ''
    return 'CREATE {}INDEX IF NOT EXISTS {} ON {} ({})'.format(
        uniq_word,
        get_index_name(table_name, index_name),
        get_table_name(table_name),
        columns_to_string(columns),
    )


def gen_insert(
    table_name: TableName,
    columns: List[Column],
    ret_column: Optional[Column] = None,
) -> str:
    v = [Column('%s') for x in columns]
    ret_sql = ' returning {}'.format(ret_column) if ret_column else ''
    return 'INSERT INTO {} ({}) VALUES ({}){}'.format(
        get_table_name(table_name),
        columns_to_string(columns),
        columns_to_string(v),
        ret_sql,
    )


def append_excluded_set(column: Column) -> str:
    col = str(column)
    if col.find('=') > -1:
        return col
    return "{} = excluded.{}".format(col, col)


def gen_insert_or_update(
    table_name: TableName,
    uniq_columns: List[Column],
    value_columns: List[Column] = [],
    other_columns: List[Column] = [],
) -> str:
    cols = uniq_columns + value_columns + other_columns
    v = [Column('%s') for x in cols]
    set_sql = ', '.join([append_excluded_set(x) for x in value_columns])
    do_sql = " DO UPDATE SET {}".format(
        set_sql) if value_columns else " DO NOTHING"
    return "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}".format(
        get_table_name(table_name), columns_to_string(cols),
        columns_to_string(v), columns_to_string(uniq_columns), do_sql)


def append_update_set(column: Column) -> str:
    col = str(column)
    if col.find('=') > -1:
        return col
    return "{} = %s".format(col)


def gen_update(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
) -> str:
    set_sql = ', '.join([append_update_set(x) for x in columns])
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    return "UPDATE {} SET {}{}".format(
        get_table_name(table_name),
        set_sql,
        where_sql,
    )


def gen_delete(
    table_name: TableName,
    part_sql: str = '',
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    return 'DELETE FROM {}{}'.format(get_table_name(table_name), where_sql)


def gen_sum(
        table_name: TableName,
        part_sql: str = '',
        column: Column = Column('*'),
        join_sql: str = '',
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    return 'SELECT sum({}) FROM {}{}{}'.format(
        str(column),
        get_table_name(table_name),
        join_sql,
        where_sql,
    )


def gen_count(
    table_name: TableName,
    part_sql: str = '',
    column: Column = Column('*'),
    join_sql: str = '',
    groups: Optional[str] = None,
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    return 'SELECT count({}) FROM {}{}{}{}'.format(
        str(column),
        get_table_name(table_name),
        join_sql,
        where_sql,
        format_group_and_sort_sql(groups, None),
    )


def gen_select(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    limit_sql = '' if size is None else ' LIMIT {}'.format(size)
    offset_sql = '' if offset is None else ' OFFSET {}'.format(offset)
    return "SELECT {} FROM {}{}{} {}{}{}".format(
        columns_to_string(columns),
        get_table_name(table_name),
        join_sql,
        where_sql,
        format_group_and_sort_sql(groups, sorts),
        limit_sql,
        offset_sql,
    )


def gen_select_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    offset: Optional[int] = None,
    size: Optional[int] = None,
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
    join_sql: str = '',
) -> str:
    return gen_select(
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        offset=offset,
        size=size,
        groups=groups,
        sorts=sorts,
        join_sql=join_sql,
    )


def gen_select_one(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    join_sql: str = '',
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    join_sql = ' {} '.format(join_sql) if join_sql else ''
    return "SELECT {} FROM {}{}{} LIMIT 1".format(
        columns_to_string(columns),
        get_table_name(table_name),
        join_sql,
        where_sql,
    )


def gen_select_one_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    join_sql: str = '',
) -> str:
    return gen_select_one(
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        join_sql=join_sql,
    )


def gen_drop_table(table_name: TableName) -> str:
    name = get_table_name(table_name)
    return f'drop table {name}'


def gen_ordering_sql(column: Column, arr: List[Any]) -> tuple[str, str]:
    ret = []
    for ordering, a in enumerate(arr):
        ret.append('({}, {})'.format(a, ordering))

    return 'JOIN (VALUES {}) AS x (id, ordering) ON {} = x.id'.format(
        ', '.join(ret), str(column)), 'ORDER BY x.ordering'


def gen_group_count(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> str:
    where_sql = ' WHERE {}'.format(part_sql) if part_sql else ''
    return "SELECT COUNT(*) FROM (SELECT {} FROM {}{}{}) G".format(
        columns_to_string(columns),
        get_table_name(table_name),
        where_sql,
        format_group_and_sort_sql(groups, sorts),
    )


def format_group_and_sort_sql(groups: str | None, sorts: str | None) -> str:
    group_sql = f' GROUP BY {groups}' if groups else ''
    sort_sql = f' ORDER BY {sorts}' if sorts else ''
    return group_sql + sort_sql
