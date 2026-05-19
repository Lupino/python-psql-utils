from collections.abc import Mapping
from typing import Optional

from ._typing import RowValue, TDefault


def get_only_default_from_row(
    ret: RowValue | None,
    default: TDefault,
    key: Optional[str] = None,
) -> object | TDefault:
    """Extract scalar value from a fetched row, with default fallback."""
    if ret is None:
        return default
    if isinstance(ret, Mapping):
        if key:
            return ret.get(key, default)
        values = iter(ret.values())
        return next(values, default)
    if len(ret) <= 0:
        return default
    return ret[0]
