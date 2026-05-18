import unittest
from typing import Any

import psql_utils
import psql_utils.sync as psql_sync


class _AsyncDummyCursor:
    def __init__(self, value: int) -> None:
        self.value = value


class _SyncDummyCursor:
    def __init__(self, value: int) -> None:
        self.value = value


@psql_utils.run_with_pool()
async def _async_add(cur: Any, x: int) -> int:
    return cur.value + x


@psql_sync.run_with_pool()
def _sync_add(cur: Any, x: int) -> int:
    return cur.value + x


class RunWithPoolAsyncContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_cursor_from_contextvar_without_connector(self) -> None:
        cur = _AsyncDummyCursor(10)
        async with psql_utils.with_cursor(cur):
            ret = await _async_add(5)
        self.assertEqual(ret, 15)

class RunWithPoolSyncContextTests(unittest.TestCase):
    def test_uses_cursor_from_contextvar_without_connector(self) -> None:
        cur = _SyncDummyCursor(10)
        with psql_sync.with_cursor(cur):
            ret = _sync_add(5)
        self.assertEqual(ret, 15)

