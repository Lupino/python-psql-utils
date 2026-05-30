from __future__ import annotations

import unittest
from typing import Any, cast

import psql_utils
import psql_utils.sync as psql_sync


class _AsyncDummyCursor:
    connection: _AsyncConn

    def __init__(self, value: int) -> None:
        self.value = value


class _SyncDummyCursor:
    connection: _SyncConn

    def __init__(self, value: int) -> None:
        self.value = value


class _AsyncCursorCM:
    def __init__(self, cur: _AsyncDummyCursor) -> None:
        self.cur = cur

    async def __aenter__(self) -> _AsyncDummyCursor:
        return self.cur

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _AsyncConn:
    def __init__(self, cur: _AsyncDummyCursor) -> None:
        self.cur = cur
        self.cursor_calls = 0
        self.transaction_calls = 0
        self.cur.connection = self

    def cursor(self, **kwargs: Any) -> _AsyncCursorCM:
        self.cursor_calls += 1
        return _AsyncCursorCM(self.cur)

    def transaction(self) -> "_AsyncTxCM":
        self.transaction_calls += 1
        return _AsyncTxCM()


class _AsyncTxCM:
    async def __aenter__(self) -> "_AsyncTxCM":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _AsyncConnCM:
    def __init__(self, conn: _AsyncConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> _AsyncConn:
        return self.conn

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _AsyncPool:
    def __init__(self, conn: _AsyncConn) -> None:
        self.conn = conn

    def connection(self) -> _AsyncConnCM:
        return _AsyncConnCM(self.conn)


class _AsyncConnector:
    def __init__(self, pool: _AsyncPool) -> None:
        self.pool = pool

    def get(self) -> _AsyncPool:
        return self.pool


class _SyncCursorCM:
    def __init__(self, cur: _SyncDummyCursor) -> None:
        self.cur = cur

    def __enter__(self) -> _SyncDummyCursor:
        return self.cur

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _SyncConn:
    def __init__(self, cur: _SyncDummyCursor) -> None:
        self.cur = cur
        self.cursor_calls = 0
        self.transaction_calls = 0
        self.cur.connection = self

    def cursor(self, **kwargs: Any) -> _SyncCursorCM:
        self.cursor_calls += 1
        return _SyncCursorCM(self.cur)

    def transaction(self) -> "_SyncTxCM":
        self.transaction_calls += 1
        return _SyncTxCM()


class _SyncTxCM:
    def __enter__(self) -> "_SyncTxCM":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _SyncConnCM:
    def __init__(self, conn: _SyncConn) -> None:
        self.conn = conn

    def __enter__(self) -> _SyncConn:
        return self.conn

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _SyncPool:
    def __init__(self, conn: _SyncConn) -> None:
        self.conn = conn

    def connection(self) -> _SyncConnCM:
        return _SyncConnCM(self.conn)


class _SyncConnector:
    def __init__(self, pool: _SyncPool) -> None:
        self.pool = pool

    def get(self) -> _SyncPool:
        return self.pool


def _get_async_dummy_cursor() -> _AsyncDummyCursor:
    cur = psql_utils.get_cursor()
    assert cur is not None
    return cast(_AsyncDummyCursor, cur)


def _get_sync_dummy_cursor() -> _SyncDummyCursor:
    cur = psql_sync.get_cursor()
    assert cur is not None
    return cast(_SyncDummyCursor, cur)


@psql_utils.run_with_pool()
async def _async_add(x: int) -> int:
    cur = _get_async_dummy_cursor()
    return cur.value + x


@psql_sync.run_with_pool()
def _sync_add(x: int) -> int:
    cur = _get_sync_dummy_cursor()
    return cur.value + x


class RunWithPoolAsyncContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_cursor_from_contextvar_without_connector(self) -> None:
        cur = _AsyncDummyCursor(10)
        async with psql_utils.with_cursor(cast(Any, cur)):
            ret = await _async_add(5)
        self.assertEqual(ret, 15)

    async def test_nested_run_with_pool_reuses_same_cursor(self) -> None:
        cur = _AsyncDummyCursor(10)
        conn = _AsyncConn(cur)
        connector = _AsyncConnector(_AsyncPool(conn))
        original_connector = psql_utils._connector
        psql_utils._connector = cast(Any, connector)

        @psql_utils.run_with_pool()
        async def inner(x: int) -> int:
            cur = _get_async_dummy_cursor()
            return cur.value + x

        @psql_utils.run_with_pool()
        async def outer(x: int) -> int:
            return await inner(x)

        try:
            ret = await outer(5)
        finally:
            psql_utils._connector = original_connector

        self.assertEqual(ret, 15)
        self.assertEqual(conn.cursor_calls, 1)

    async def test_transaction_true_skips_existing_scoped_cursor(self) -> None:
        conn = _AsyncConn(_AsyncDummyCursor(10))
        cur = conn.cur

        @psql_utils.run_with_pool(transaction=True)
        async def add(x: int) -> int:
            c = _get_async_dummy_cursor()
            return c.value + x

        async with psql_utils.with_cursor(cast(Any, cur)):
            ret = await add(5)

        self.assertEqual(ret, 15)
        self.assertEqual(conn.transaction_calls, 0)

    async def test_transaction_true_uses_transaction_for_new_cursor(
        self,
    ) -> None:
        cur = _AsyncDummyCursor(10)
        conn = _AsyncConn(cur)
        connector = _AsyncConnector(_AsyncPool(conn))
        original_connector = psql_utils._connector
        psql_utils._connector = cast(Any, connector)

        @psql_utils.run_with_pool(transaction=True)
        async def add(x: int) -> int:
            c = _get_async_dummy_cursor()
            return c.value + x

        try:
            ret = await add(5)
        finally:
            psql_utils._connector = original_connector

        self.assertEqual(ret, 15)
        self.assertEqual(conn.cursor_calls, 1)
        self.assertEqual(conn.transaction_calls, 1)


class RunWithPoolSyncContextTests(unittest.TestCase):
    def test_uses_cursor_from_contextvar_without_connector(self) -> None:
        cur = _SyncDummyCursor(10)
        with psql_sync.with_cursor(cast(Any, cur)):
            ret = _sync_add(5)
        self.assertEqual(ret, 15)

    def test_nested_run_with_pool_reuses_same_cursor(self) -> None:
        cur = _SyncDummyCursor(10)
        conn = _SyncConn(cur)
        connector = _SyncConnector(_SyncPool(conn))
        original_connector = psql_sync._connector
        psql_sync._connector = cast(Any, connector)

        @psql_sync.run_with_pool()
        def inner(x: int) -> int:
            cur = _get_sync_dummy_cursor()
            return cur.value + x

        @psql_sync.run_with_pool()
        def outer(x: int) -> int:
            return inner(x)

        try:
            ret = outer(5)
        finally:
            psql_sync._connector = original_connector

        self.assertEqual(ret, 15)
        self.assertEqual(conn.cursor_calls, 1)

    def test_transaction_true_skips_existing_scoped_cursor(self) -> None:
        conn = _SyncConn(_SyncDummyCursor(10))
        cur = conn.cur

        @psql_sync.run_with_pool(transaction=True)
        def add(x: int) -> int:
            c = _get_sync_dummy_cursor()
            return c.value + x

        with psql_sync.with_cursor(cast(Any, cur)):
            ret = add(5)

        self.assertEqual(ret, 15)
        self.assertEqual(conn.transaction_calls, 0)

    def test_transaction_true_uses_transaction_for_new_cursor(self) -> None:
        cur = _SyncDummyCursor(10)
        conn = _SyncConn(cur)
        connector = _SyncConnector(_SyncPool(conn))
        original_connector = psql_sync._connector
        psql_sync._connector = cast(Any, connector)

        @psql_sync.run_with_pool(transaction=True)
        def add(x: int) -> int:
            c = _get_sync_dummy_cursor()
            return c.value + x

        try:
            ret = add(5)
        finally:
            psql_sync._connector = original_connector

        self.assertEqual(ret, 15)
        self.assertEqual(conn.cursor_calls, 1)
        self.assertEqual(conn.transaction_calls, 1)
