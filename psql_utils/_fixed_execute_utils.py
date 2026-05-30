from collections.abc import Mapping

from ._typing import Description, RowDict, Rows, RowValue, SQLArgs


def has_sql_args(args: SQLArgs | None) -> bool:
    """Return True when SQL args should be passed into cursor.execute()."""
    if not args:
        return False
    try:
        return len(args) > 0
    except TypeError:
        # Non-sized but truthy args (e.g. generators) should still be passed.
        return True


def _description_names(description: Description) -> list[str]:
    if not description:
        raise ValueError('cursor description is required for dict rows')
    return [d.name for d in description]


def row_to_dict(row: RowValue, description: Description) -> RowDict:
    """Convert a fetched row to dict using cursor description when needed."""
    if isinstance(row, Mapping):
        return dict(row)

    cols = _description_names(description)
    if len(cols) != len(row):
        raise ValueError(
            'row length does not match cursor description: '
            f'{len(row)} values for {len(cols)} columns')
    return dict(zip(cols, row))


def rows_to_dicts(rows: Rows | None,
                  description: Description) -> list[RowDict]:
    """Convert fetched rows to list[dict]."""
    if not rows:
        return []
    return [row_to_dict(row, description) for row in rows]
