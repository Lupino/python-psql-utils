import unittest
from types import SimpleNamespace
from typing import Any, List, Optional, cast

import psql_utils
import psql_utils.sync as psql_sync
from psql_utils.types import c, t


class _SyncCursor:
    def __init__(
        self,
        rows: Optional[List[Any]] = None,
        description: Optional[List[Any]] = None,
    ) -> None:
        self._rows = rows or []
        self.description = description
        self.executed: List[tuple[str, Any]] = []
        self.rowcount = 0

    def execute(self, sql: str, args: Any = None) -> None:
        self.executed.append((sql, args))
        self.rowcount = 1

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
        self.executed: List[tuple[str, Any]] = []
        self.rowcount = 0

    async def execute(self, sql: str, args: Any = None) -> None:
        self.executed.append((sql, args))
        self.rowcount = 1

    async def fetchall(self) -> List[Any]:
        return self._rows

    async def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


class FixedExecuteSyncTests(unittest.TestCase):
    def test_fetch_all_as_dict_from_tuple_rows(self) -> None:
        cur = _SyncCursor(
            rows=[(1, "alice"), (2, "bob")],
            description=[
                SimpleNamespace(name="id"),
                SimpleNamespace(name="name"),
            ],
        )
        with psql_sync.with_cursor(cast(Any, cur)):
            ret = psql_sync.fixed_execute(
                "select id, name from users",
                fetch="all",
                as_dict=True,
            )
        self.assertEqual(
            ret,
            [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        )

    def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _SyncCursor()
        args = (x for x in [1])
        with psql_sync.with_cursor(cast(Any, cur)):
            psql_sync.fixed_execute("select %s", args=cast(Any, args))
        self.assertEqual(cur.executed, [("select %s", args)])

    def test_invalid_fetch_mode_raises(self) -> None:
        cur = _SyncCursor()
        with psql_sync.with_cursor(cast(Any, cur)):
            with self.assertRaises(ValueError):
                psql_sync.fixed_execute("select 1", fetch=cast(Any, "many"))

    def test_update_returns_cursor_rowcount(self) -> None:
        cur = _SyncCursor()
        with psql_sync.with_cursor(cast(Any, cur)):
            ret = psql_sync.update(
                t("users"),
                [c("name")],
                part_sql="id=%s",
                args=("alice", 1),
            )
        self.assertEqual(ret, 1)


class FixedExecuteAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_all_as_dict_from_tuple_rows(self) -> None:
        cur = _AsyncCursor(
            rows=[(1, "alice"), (2, "bob")],
            description=[
                SimpleNamespace(name="id"),
                SimpleNamespace(name="name"),
            ],
        )
        async with psql_utils.with_cursor(cast(Any, cur)):
            ret = await psql_utils.fixed_execute(
                "select id, name from users",
                fetch="all",
                as_dict=True,
            )
        self.assertEqual(
            ret,
            [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
        )

    async def test_non_sized_truthy_args_are_forwarded(self) -> None:
        cur = _AsyncCursor()
        args = (x for x in [1])
        async with psql_utils.with_cursor(cast(Any, cur)):
            await psql_utils.fixed_execute("select %s", args=cast(Any, args))
        self.assertEqual(cur.executed, [("select %s", args)])

    async def test_invalid_fetch_mode_raises(self) -> None:
        cur = _AsyncCursor()
        async with psql_utils.with_cursor(cast(Any, cur)):
            with self.assertRaises(ValueError):
                await psql_utils.fixed_execute(
                    "select 1",
                    fetch=cast(Any, "many"),
                )

    async def test_update_returns_cursor_rowcount(self) -> None:
        cur = _AsyncCursor()
        async with psql_utils.with_cursor(cast(Any, cur)):
            ret = await psql_utils.update(
                t("users"),
                [c("name")],
                part_sql="id=%s",
                args=("alice", 1),
            )
        self.assertEqual(ret, 1)
