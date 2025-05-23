from typing import Optional, List


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


def constraint_primary_key(
    table_name: TableName,
    columns: List[Column],
) -> Column:
    return Column('CONSTRAINT {} PRIMARY KEY ({})'.format(
        get_index_name(table_name, i('pk')),
        columns_to_string(columns),
    ))
