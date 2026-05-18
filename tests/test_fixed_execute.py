import unittest
from types import SimpleNamespace
from typing import Any, List, Optional

import psql_utils
import psql_utils.sync as psql_sync


class _SyncCursor:
    def __init__(
        self,
        rows: Optional[List[Any]] = None,
        description: Optional[List[Any]] = None,
    ) -> None:
        self._rows = rows or []
        self.description = description
        self.executed = []

    def execute(self, sql: str, args: Any = None) -> None:
        self.executed.append((sql, args))

    def fetchall(self) -> List[Any]:
        return self._rows

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


class _AsyncCursor:
    def __init__(
        self,
        rows: Optional[List[Any]] = None,
        description: Optional[List[Any]] = None,
    ) -> None:
        self._rows = rows or []
        self.description = description
        self.executed = []

    async def execute(self, sql: str, args: Any = None) -> None:
        self.executed.append((sql, args))

    async def fetchall(self) -> List[Any]:
        return self._rows

    async def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


class _SyncSingleRowCursor:
    def __init__(self, row: Any) -> None:
        self._row = row

    def fetchone(self) -> Any:
        return self._row


class _AsyncSingleRowCursor:
    def __init__(self, row: Any) -> None:
        self._row = row

    async def fetchone(self) -> Any:
        return self._row


class FixedExecuteSyncTests(unittest.TestCase):
    def test_fetch_all_as_dict_from_tuple_rows(self) -> None:
        cur = _SyncCursor(
            rows=[(1, "alice"), (2, "bob")],
            description=[SimpleNamespace(name="id"), SimpleNamespace(name="name")],
        )
        with psql_sync.with_cursor(cur):
            ret = psql_sync.fixed_execute(
                "select id, name from users",
                fetch="all",
                as_dict=True,
            )
        self.assertEqual(ret, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _SyncCursor()
        args = (x for x in [1])
        with psql_sync.with_cursor(cur):
            psql_sync.fixed_execute("select %s", args=args)
        self.assertEqual(cur.executed, [("select %s", args)])

    def test_invalid_fetch_mode_raises(self) -> None:
        cur = _SyncCursor()
        with psql_sync.with_cursor(cur):
            with self.assertRaises(ValueError):
                psql_sync.fixed_execute("select 1", fetch="many")

    def test_get_only_default_returns_scalar_or_default(self) -> None:
        with psql_sync.with_cursor(_SyncSingleRowCursor((7,))):
            self.assertEqual(psql_sync.get_only_default(0), 7)
        with psql_sync.with_cursor(_SyncSingleRowCursor(None)):
            self.assertEqual(psql_sync.get_only_default(9), 9)

    def test_get_only_default_dict_with_key(self) -> None:
        with psql_sync.with_cursor(_SyncSingleRowCursor({"n": 3})):
            self.assertEqual(psql_sync.get_only_default(0, "n"), 3)
        with psql_sync.with_cursor(_SyncSingleRowCursor({"x": 1})):
            self.assertEqual(psql_sync.get_only_default(0, "n"), 0)


class FixedExecuteAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_all_as_dict_from_tuple_rows(self) -> None:
        cur = _AsyncCursor(
            rows=[(1, "alice"), (2, "bob")],
            description=[SimpleNamespace(name="id"), SimpleNamespace(name="name")],
        )
        async with psql_utils.with_cursor(cur):
            ret = await psql_utils.fixed_execute(
                "select id, name from users",
                fetch="all",
                as_dict=True,
            )
        self.assertEqual(ret, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    async def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _AsyncCursor()
        args = (x for x in [1])
        async with psql_utils.with_cursor(cur):
            await psql_utils.fixed_execute("select %s", args=args)
        self.assertEqual(cur.executed, [("select %s", args)])

    async def test_invalid_fetch_mode_raises(self) -> None:
        cur = _AsyncCursor()
        async with psql_utils.with_cursor(cur):
            with self.assertRaises(ValueError):
                await psql_utils.fixed_execute("select 1", fetch="many")

    async def test_get_only_default_returns_scalar_or_default(self) -> None:
        async with psql_utils.with_cursor(_AsyncSingleRowCursor((7,))):
            self.assertEqual(await psql_utils.get_only_default(0), 7)
        async with psql_utils.with_cursor(_AsyncSingleRowCursor(None)):
            self.assertEqual(await psql_utils.get_only_default(9), 9)

    async def test_get_only_default_dict_with_key(self) -> None:
        async with psql_utils.with_cursor(_AsyncSingleRowCursor({"n": 3})):
            self.assertEqual(await psql_utils.get_only_default(0, "n"), 3)
        async with psql_utils.with_cursor(_AsyncSingleRowCursor({"x": 1})):
            self.assertEqual(await psql_utils.get_only_default(0, "n"), 0)
