from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, TypeVar

try:
    from typing import Concatenate
except ImportError:  # pragma: no cover
    from typing_extensions import Concatenate

try:
    from typing import ParamSpec
except ImportError:  # pragma: no cover
    from typing_extensions import ParamSpec

# SQL execution args accepted by psycopg execute().
SQLArgs = Mapping[str, object] | Sequence[object]


# Cursor description items expose a "name" attribute.
class CursorDescriptionItem(Protocol):
    name: str


Description = Sequence[CursorDescriptionItem] | None

# Generic row value returned by psycopg fetch methods.
RowValue = Mapping[str, object] | Sequence[object]
Rows = Sequence[RowValue]
RowDict = dict[str, object]

TDefault = TypeVar("TDefault")
P = ParamSpec("P")
R = TypeVar("R")

__all__ = [
    "Concatenate",
    "ParamSpec",
    "SQLArgs",
    "CursorDescriptionItem",
    "Description",
    "RowValue",
    "Rows",
    "RowDict",
    "TDefault",
    "P",
    "R",
]
