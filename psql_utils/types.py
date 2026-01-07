from typing import List, Optional, Union


class LeftJoin:
    """Represents a LEFT JOIN clause in SQL."""

    def __init__(self, table_node: 'TableName', where: str) -> None:
        # Renamed 'table_name' to 'table_node' internally to avoid confusion
        # between the class instance and the string name, though types match.
        self.table_node = table_node
        self.where = where

    def __str__(self) -> str:
        return f'LEFT JOIN {self.table_node} ON {self.where}'


class TableName:
    """
    Represents a SQL table, including its alias and any joined tables.
    """
    table_name: str
    alias_name: Optional[str]
    joins: List[LeftJoin]

    def __init__(
        self,
        table_name: str,
        alias: Optional[str] = None,
        joins: Optional[List[LeftJoin]] = None,
    ) -> None:
        self.table_name = table_name
        self.alias_name = alias
        # Fix: Use None as default to avoid mutable default argument issues
        self.joins = joins if joins is not None else []

    def alias(self, alias: str) -> 'TableName':
        """Returns a new TableName instance with the specified alias."""
        # Create a copy of the joins list to maintain immutability
        return TableName(self.table_name, alias, self.joins[:])

    def join(self, table: 'TableName', where: str) -> 'TableName':
        """Adds a LEFT JOIN to the table."""
        new_joins = self.joins[:]
        new_joins.append(LeftJoin(table, where))
        return TableName(self.table_name, self.alias_name, new_joins)

    def __str__(self) -> str:
        # Handle table name quoting and aliasing
        if self.alias_name is None:
            base_name = f'"{self.table_name}"'
        else:
            base_name = f'"{self.table_name}" AS {self.alias_name}'

        # Append joins if they exist
        if self.joins:
            join_str = ' '.join(str(join) for join in self.joins)
            return f'{base_name} {join_str}'

        return base_name


def get_table_name(table_node: Union[List[TableName], TableName]) -> str:
    """Converts a TableName or list of TableNames to a string."""
    if isinstance(table_node, list):
        return ', '.join(str(tn) for tn in table_node)
    return str(table_node)


def t(table_name: str) -> TableName:
    """Shorthand factory for creating a TableName."""
    return TableName(table_name)


class Column:
    """Represents a SQL column."""
    column: str

    def __init__(self, column: str) -> None:
        self.column = column

    def __str__(self) -> str:
        return self.column


def c(column: str) -> Column:
    """Shorthand factory for creating a Column."""
    return Column(column)


# Pre-defined wildcard column
c_all = c('*')


def cs(columns: List[str]) -> List[Column]:
    """Converts a list of strings to a list of Column objects."""
    return [c(col) for col in columns]


cs_all = cs(['*'])


def columns_to_string(columns: List[Column]) -> str:
    """Joins a list of Column objects into a comma-separated string."""
    return ', '.join(str(col) for col in columns)


class IndexName:
    """Represents a SQL index name."""
    index_name: str

    def __init__(self, index_name: str) -> None:
        self.index_name = index_name

    def __str__(self) -> str:
        return self.index_name


def i(index_name: str) -> IndexName:
    """Shorthand factory for creating an IndexName."""
    return IndexName(index_name)


def get_index_name(table_node: TableName, index_node: IndexName) -> str:
    """Formats an index name based on the table and index identifier."""
    return f'"{table_node.table_name}_{index_node.index_name}"'


def constraint_primary_key(
    table_node: TableName,
    columns: List[Column],
) -> Column:
    """Creates a PRIMARY KEY constraint definition."""
    pk_name = get_index_name(table_node, i('pk'))
    cols_str = columns_to_string(columns)

    return Column(f'CONSTRAINT {pk_name} PRIMARY KEY ({cols_str})')
