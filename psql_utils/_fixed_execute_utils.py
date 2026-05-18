from typing import Any, Dict, List


def has_sql_args(args: Any) -> bool:
    """Return True when SQL args should be passed into cursor.execute()."""
    if not args:
        return False
    try:
        return len(args) > 0
    except TypeError:
        # Non-sized but truthy args (e.g. generators) should still be passed.
        return True


def row_to_dict(row: Any, description: Any) -> Dict[str, Any]:
    """Convert a fetched row to dict using cursor description when needed."""
    row_any: Any = row
    if isinstance(row_any, dict):
        return dict(row_any)
    cols = [d.name for d in (description or [])]
    return dict(zip(cols, row))


def rows_to_dicts(rows: Any, description: Any) -> List[Dict[str, Any]]:
    """Convert fetched rows to list[dict]."""
    if not rows:
        return []
    first_any: Any = rows[0]
    if isinstance(first_any, dict):
        return [dict(row) for row in rows]
    cols = [d.name for d in (description or [])]
    return [dict(zip(cols, row)) for row in rows]
