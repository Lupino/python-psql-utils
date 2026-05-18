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


def row_to_dict(row: RowValue, description: Description) -> RowDict:
    """Convert a fetched row to dict using cursor description when needed."""
    if isinstance(row, Mapping):
        return dict(row)
    cols = [d.name for d in (description or ())]
    return dict(zip(cols, row))


def rows_to_dicts(rows: Rows | None,
                  description: Description) -> list[RowDict]:
    """Convert fetched rows to list[dict]."""
    if not rows:
        return []
    first = rows[0]
    if isinstance(first, Mapping):
        return [
            dict(row) if isinstance(row, Mapping) else row_to_dict(
                row, description) for row in rows
        ]
    cols = [d.name for d in (description or ())]
    return [dict(zip(cols, row)) for row in rows]
