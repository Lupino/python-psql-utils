from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol, TypeVar

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
