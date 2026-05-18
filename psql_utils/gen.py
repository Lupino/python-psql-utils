import re
from typing import Optional, List, Any, Tuple

from .types import (TableName, Column, IndexName, get_table_name,
                    columns_to_string, get_index_name)

RE_UNSAFE_SQL = re.compile(r';|--|/\*|\*/')


def _validate_sql_fragment(name: str, sql: str) -> None:
    if '\x00' in sql:
        raise ValueError(f'{name} contains null byte')
    if RE_UNSAFE_SQL.search(sql):
        raise ValueError(f'{name} contains unsafe token')


def gen_create_table(
    table_name: TableName,
    columns: List[Column],
) -> str:
    """Generates a CREATE TABLE statement."""
    return 'CREATE TABLE IF NOT EXISTS {} ({})'.format(
        get_table_name(table_name),
        columns_to_string(columns),
    )


def gen_add_table_column(
    table_name: TableName,
    columns: List[Column],
) -> str:
    """Generates an ALTER TABLE ADD COLUMN statement."""
    return 'ALTER TABLE {} ADD COLUMN {}'.format(
        get_table_name(table_name),
        columns_to_string(columns),
    )


def gen_create_index(
    uniq: bool,
    table_name: TableName,
    index_name: IndexName,
    columns: List[Column],
) -> str:
    """Generates a CREATE INDEX statement."""
    uniq_word = 'UNIQUE ' if uniq else ''
    idx_name = get_index_name(table_name, index_name)
    tbl_name = get_table_name(table_name)
    cols = columns_to_string(columns)

    check = 'IF NOT EXISTS'

    return f'CREATE {uniq_word}INDEX {check} {idx_name} ON {tbl_name} ({cols})'


def gen_insert(
    table_name: TableName,
    columns: List[Column],
    ret_column: Optional[Column] = None,
) -> str:
    """Generates an INSERT statement."""
    placeholders = columns_to_string([Column('%s') for _ in columns])
    ret_sql = f' RETURNING {ret_column}' if ret_column else ''

    return 'INSERT INTO {} ({}) VALUES ({}){}'.format(
        get_table_name(table_name),
        columns_to_string(columns),
        placeholders,
        ret_sql,
    )


def append_excluded_set(column: Column) -> str:
    """Helper to format ON CONFLICT SET clauses."""
    col_str = str(column)
    if '=' in col_str:
        return col_str
    return f"{col_str} = excluded.{col_str}"


def gen_insert_or_update(
    table_name: TableName,
    uniq_columns: List[Column],
    value_columns: Optional[List[Column]] = None,
    other_columns: Optional[List[Column]] = None,
) -> str:
    """Generates an INSERT ... ON CONFLICT DO UPDATE statement."""
    if value_columns is None:
        value_columns = []
    if other_columns is None:
        other_columns = []

    all_cols = uniq_columns + value_columns + other_columns
    placeholders = columns_to_string([Column('%s') for _ in all_cols])

    if value_columns:
        set_items = [append_excluded_set(x) for x in value_columns]
        do_sql = f" DO UPDATE SET {', '.join(set_items)}"
    else:
        do_sql = " DO NOTHING"

    return (f"INSERT INTO {get_table_name(table_name)} "
            f"({columns_to_string(all_cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({columns_to_string(uniq_columns)}){do_sql}")


def append_update_set(column: Column) -> str:
    """Helper to format UPDATE SET clauses."""
    col_str = str(column)
    if '=' in col_str:
        return col_str
    return f"{col_str} = %s"


def gen_update(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
) -> str:
    """Generates an UPDATE statement."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    set_sql = ', '.join([append_update_set(x) for x in columns])
    where_sql = f' WHERE {part_sql}' if part_sql else ''

    return f"UPDATE {get_table_name(table_name)} SET {set_sql}{where_sql}"


def gen_delete(
    table_name: TableName,
    part_sql: str = '',
) -> str:
    """Generates a DELETE statement."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    return f'DELETE FROM {get_table_name(table_name)}{where_sql}'


def gen_sum(
        table_name: TableName,
        part_sql: str = '',
        column: Column = Column('*'),
        join_sql: str = '',
) -> str:
    """Generates a SELECT SUM(...) statement."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    if join_sql:
        _validate_sql_fragment('join_sql', join_sql)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    join_sql = f' {join_sql} ' if join_sql else ''

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
    """Generates a SELECT COUNT(...) statement."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    if join_sql:
        _validate_sql_fragment('join_sql', join_sql)
    if groups:
        _validate_sql_fragment('groups', groups)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    join_sql = f' {join_sql} ' if join_sql else ''

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
    lock_sql: str = '',
) -> str:
    """Generates a SELECT statement."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    if join_sql:
        _validate_sql_fragment('join_sql', join_sql)
    if groups:
        _validate_sql_fragment('groups', groups)
    if sorts:
        _validate_sql_fragment('sorts', sorts)
    if lock_sql:
        _validate_sql_fragment('lock_sql', lock_sql)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    join_sql = f' {join_sql} ' if join_sql else ''
    limit_sql = f' LIMIT {size}' if size is not None else ''
    offset_sql = f' OFFSET {offset}' if offset is not None else ''
    lock_sql = f' {lock_sql}' if lock_sql else ''

    return "SELECT {} FROM {}{}{} {}{}{}{}".format(
        columns_to_string(columns),
        get_table_name(table_name),
        join_sql,
        where_sql,
        format_group_and_sort_sql(groups, sorts),
        limit_sql,
        offset_sql,
        lock_sql,
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
    lock_sql: str = '',
) -> str:
    """Wrapper for gen_select to select a single column."""
    return gen_select(
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        offset=offset,
        size=size,
        groups=groups,
        sorts=sorts,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )


def gen_select_one(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    join_sql: str = '',
    lock_sql: str = '',
) -> str:
    """Wrapper for gen_select to limit result to 1."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    if join_sql:
        _validate_sql_fragment('join_sql', join_sql)
    if lock_sql:
        _validate_sql_fragment('lock_sql', lock_sql)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    join_sql = f' {join_sql} ' if join_sql else ''
    lock_sql = f' {lock_sql}' if lock_sql else ''

    return "SELECT {} FROM {}{}{} LIMIT 1{}".format(
        columns_to_string(columns),
        get_table_name(table_name),
        join_sql,
        where_sql,
        lock_sql,
    )


def gen_select_one_only(
    table_name: TableName,
    column: Column,
    part_sql: str = '',
    join_sql: str = '',
    lock_sql: str = '',
) -> str:
    """Wrapper for gen_select_one to select a single column."""
    return gen_select_one(
        table_name=table_name,
        columns=[column],
        part_sql=part_sql,
        join_sql=join_sql,
        lock_sql=lock_sql,
    )


def gen_drop_table(table_name: TableName) -> str:
    """Generates a DROP TABLE statement."""
    return f'DROP TABLE {get_table_name(table_name)}'


def gen_ordering_sql(column: Column, arr: List[Any]) -> Tuple[str, str]:
    """Generates a JOIN clause for arbitrary ordering based on values."""
    values_list = []
    for order, val in enumerate(arr):
        values_list.append(f'({val}, {order})')

    values_str = ', '.join(values_list)
    join_sql = (f'JOIN (VALUES {values_str}) AS x (id, ordering) '
                f'ON {column} = x.id')
    return join_sql, 'x.ordering'


def gen_group_count(
    table_name: TableName,
    columns: List[Column],
    part_sql: str = '',
    groups: Optional[str] = None,
    sorts: Optional[str] = None,
) -> str:
    """Generates a count of rows in a subquery."""
    if part_sql:
        _validate_sql_fragment('part_sql', part_sql)
    if groups:
        _validate_sql_fragment('groups', groups)
    if sorts:
        _validate_sql_fragment('sorts', sorts)
    where_sql = f' WHERE {part_sql}' if part_sql else ''
    group_sort = format_group_and_sort_sql(groups, sorts)

    return (f"SELECT COUNT(*) FROM (SELECT {columns_to_string(columns)} "
            f"FROM {get_table_name(table_name)}{where_sql}{group_sort}) G")


def format_group_and_sort_sql(groups: Optional[str],
                              sorts: Optional[str]) -> str:
    """Helpers to format GROUP BY and ORDER BY clauses."""
    group_sql = f' GROUP BY {groups}' if groups else ''
    sort_sql = f' ORDER BY {sorts}' if sorts else ''
    return group_sql + sort_sql
