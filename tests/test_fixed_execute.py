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
        ret = psql_sync.fixed_execute(
            cur,
            "select id, name from users",
            fetch="all",
            as_dict=True,
        )
        self.assertEqual(ret, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _SyncCursor()
        args = (x for x in [1])
        psql_sync.fixed_execute(cur, "select %s", args=args)
        self.assertEqual(cur.executed, [("select %s", args)])

    def test_invalid_fetch_mode_raises(self) -> None:
        cur = _SyncCursor()
        with self.assertRaises(ValueError):
            psql_sync.fixed_execute(cur, "select 1", fetch="many")

    def test_get_only_default_returns_scalar_or_default(self) -> None:
        self.assertEqual(
            psql_sync.get_only_default(_SyncSingleRowCursor((7,)), 0),
            7,
        )
        self.assertEqual(
            psql_sync.get_only_default(_SyncSingleRowCursor(None), 9),
            9,
        )

    def test_get_only_default_dict_with_key(self) -> None:
        self.assertEqual(
            psql_sync.get_only_default(_SyncSingleRowCursor({"n": 3}), 0, "n"),
            3,
        )
        self.assertEqual(
            psql_sync.get_only_default(_SyncSingleRowCursor({"x": 1}), 0, "n"),
            0,
        )


class FixedExecuteAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_all_as_dict_from_tuple_rows(self) -> None:
        cur = _AsyncCursor(
            rows=[(1, "alice"), (2, "bob")],
            description=[SimpleNamespace(name="id"), SimpleNamespace(name="name")],
        )
        ret = await psql_utils.fixed_execute(
            cur,
            "select id, name from users",
            fetch="all",
            as_dict=True,
        )
        self.assertEqual(ret, [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}])

    async def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _AsyncCursor()
        args = (x for x in [1])
        await psql_utils.fixed_execute(cur, "select %s", args=args)
        self.assertEqual(cur.executed, [("select %s", args)])

    async def test_invalid_fetch_mode_raises(self) -> None:
        cur = _AsyncCursor()
        with self.assertRaises(ValueError):
            await psql_utils.fixed_execute(cur, "select 1", fetch="many")

    async def test_get_only_default_returns_scalar_or_default(self) -> None:
        self.assertEqual(
            await psql_utils.get_only_default(_AsyncSingleRowCursor((7,)), 0),
            7,
        )
        self.assertEqual(
            await psql_utils.get_only_default(_AsyncSingleRowCursor(None), 9),
            9,
        )

    async def test_get_only_default_dict_with_key(self) -> None:
        self.assertEqual(
            await psql_utils.get_only_default(
                _AsyncSingleRowCursor({"n": 3}),
                0,
                "n",
            ),
            3,
        )
        self.assertEqual(
            await psql_utils.get_only_default(
                _AsyncSingleRowCursor({"x": 1}),
                0,
                "n",
            ),
            0,
        )
