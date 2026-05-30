import asyncio
from types import SimpleNamespace
from typing import Any, cast
import unittest
from unittest.mock import patch

import psql_utils
from psql_utils._fixed_execute_utils import row_to_dict, rows_to_dicts
from psql_utils.types import c, t


class FakeCursor:

    def __init__(self) -> None:
        self.description = [
            SimpleNamespace(name='id'),
            SimpleNamespace(name='name'),
        ]
        self.executed: list[tuple[str, object | None]] = []
        self.rowcount = 1

    async def __aenter__(self) -> 'FakeCursor':
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, sql: str, args: object | None = None) -> None:
        self.executed.append((sql, args))

    async def fetchone(self) -> tuple[int, str]:
        return (1, 'alpha')

    async def fetchall(self) -> list[tuple[int, str]]:
        return [(1, 'alpha'), (2, 'beta')]


class FakeConnection:

    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.cursor_calls = 0

    async def __aenter__(self) -> 'FakeConnection':
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        self.cursor_calls += 1
        return self._cursor


class FakePool:

    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection
        self.connection_calls = 0

    def connection(self) -> FakeConnection:
        self.connection_calls += 1
        return self._connection


class FakeConnector:

    def __init__(self, pool: FakePool) -> None:
        self._pool = pool

    def get(self) -> FakePool:
        return self._pool

    async def connect(self) -> bool:
        return True


class PoolExecutionTests(unittest.TestCase):

    def test_fixed_execute_opens_pool_when_no_cursor(self) -> None:
        cursor = FakeCursor()
        connection = FakeConnection(cursor)
        pool = FakePool(connection)
        connector = FakeConnector(pool)

        with patch.object(psql_utils, '_connector', connector):
            row = asyncio.run(
                psql_utils.fixed_execute(
                    'SELECT id, name FROM fake WHERE id=%s',
                    (1, ),
                    fetch='one',
                    as_dict=True,
                ))

        self.assertEqual(row, {
            'id': 1,
            'name': 'alpha',
        })
        self.assertEqual(pool.connection_calls, 1)
        self.assertEqual(connection.cursor_calls, 1)
        self.assertEqual(cursor.executed,
                         [('SELECT id, name FROM fake WHERE id=%s', (1, ))])
        self.assertIsNone(psql_utils.get_cursor())

    def test_fixed_execute_reuses_scoped_cursor(self) -> None:
        cursor = FakeCursor()
        pool = FakePool(FakeConnection(FakeCursor()))
        connector = FakeConnector(pool)

        async def run() -> Any:
            async with psql_utils.with_cursor(cast(Any, cursor)):
                return await psql_utils.fixed_execute(
                    'SELECT id, name FROM fake',
                    fetch='all',
                    as_dict=True,
                )

        with patch.object(psql_utils, '_connector', connector):
            rows = asyncio.run(run())

        self.assertEqual(rows, [
            {
                'id': 1,
                'name': 'alpha',
            },
            {
                'id': 2,
                'name': 'beta',
            },
        ])
        self.assertEqual(pool.connection_calls, 0)
        self.assertEqual(cursor.executed,
                         [('SELECT id, name FROM fake', None)])

    def test_select_returns_dicts_without_row_factory(self) -> None:
        cursor = FakeCursor()
        connection = FakeConnection(cursor)
        pool = FakePool(connection)
        connector = FakeConnector(pool)

        with patch.object(psql_utils, '_connector', connector):
            rows = asyncio.run(
                psql_utils.select(
                    table_name=t('fake_table'),
                    columns=[c('id'), c('name')],
                ))

        self.assertEqual(rows, [
            {
                'id': 1,
                'name': 'alpha',
            },
            {
                'id': 2,
                'name': 'beta',
            },
        ])
        self.assertEqual(pool.connection_calls, 1)
        self.assertEqual(connection.cursor_calls, 1)


class RowConversionTests(unittest.TestCase):

    def test_row_to_dict_preserves_mapping_rows_without_description(
            self) -> None:
        self.assertEqual(row_to_dict({'id': 1, 'name': 'alpha'}, None), {
            'id': 1,
            'name': 'alpha',
        })

    def test_row_to_dict_rejects_missing_description_for_tuple_row(
            self) -> None:
        with self.assertRaisesRegex(ValueError,
                                    'cursor description is required'):
            row_to_dict((1, 'alpha'), None)

    def test_row_to_dict_rejects_description_length_mismatch(self) -> None:
        description = [SimpleNamespace(name='id')]

        with self.assertRaisesRegex(ValueError, 'row length does not match'):
            row_to_dict((1, 'alpha'), description)

    def test_rows_to_dicts_converts_each_row_independently(self) -> None:
        description = [
            SimpleNamespace(name='id'),
            SimpleNamespace(name='name'),
        ]

        self.assertEqual(rows_to_dicts([{'id': 1}, (2, 'beta')], description),
                         [
                             {
                                 'id': 1,
                             },
                             {
                                 'id': 2,
                                 'name': 'beta',
                             },
                         ])
