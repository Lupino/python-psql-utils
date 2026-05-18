from typing import Any, Optional


def get_only_default_from_row(
    ret: Any,
    default: Any,
    key: Optional[str] = None,
) -> Any:
    """Extract scalar value from a fetched row, with default fallback."""
    if ret is None:
        return default
    ret_any: Any = ret
    if key and isinstance(ret_any, dict):
        return ret_any.get(key, default)
    return ret[0]
