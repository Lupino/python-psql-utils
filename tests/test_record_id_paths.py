import unittest
from unittest.mock import AsyncMock, patch

from psql_utils import record
from psql_utils import record_sync
from psql_utils.types import t


class RecordIdPathTests(unittest.IsolatedAsyncioTestCase):

    async def test_async_get_with_id_zero_uses_id_path(self) -> None:
        with patch("psql_utils.record.select_one",
                   new_callable=AsyncMock) as m:
            m.return_value = {"id": 0}
            ret = await record.get(t("users"), id=0)
            self.assertEqual(ret, {"id": 0})
            _, kwargs = m.call_args
            self.assertEqual(kwargs["part_sql"], "id=%s")
            self.assertEqual(kwargs["args"], [0])

    async def test_async_save_with_id_zero_checks_existing_by_id(self) -> None:
        with patch("psql_utils.record.get", new_callable=AsyncMock) as m_get:
            with patch("psql_utils.record.update",
                       new_callable=AsyncMock) as m_upd:
                m_get.return_value = {"id": 0, "name": "old"}
                ret = await record.save(
                    t("users"),
                    id=0,
                    keys=["name"],
                    name="new",
                )
                self.assertEqual(ret, 0)
                _, get_kwargs = m_get.call_args
                self.assertEqual(get_kwargs, {"id": 0})
                m_upd.assert_awaited_once()


class RecordSyncIdPathTests(unittest.TestCase):

    def test_sync_get_with_id_zero_uses_id_path(self) -> None:
        with patch("psql_utils.record_sync.select_one") as m:
            m.return_value = {"id": 0}
            ret = record_sync.get(t("users"), id=0)
            self.assertEqual(ret, {"id": 0})
            _, kwargs = m.call_args
            self.assertEqual(kwargs["part_sql"], "id=%s")
            self.assertEqual(kwargs["args"], [0])

    def test_sync_save_with_id_zero_checks_existing_by_id(self) -> None:
        with patch("psql_utils.record_sync.get") as m_get:
            with patch("psql_utils.record_sync.update") as m_upd:
                m_get.return_value = {"id": 0, "name": "old"}
                ret = record_sync.save(t("users"),
                                       id=0,
                                       keys=["name"],
                                       name="new")
                self.assertEqual(ret, 0)
                _, get_kwargs = m_get.call_args
                self.assertEqual(get_kwargs, {"id": 0})
                m_upd.assert_called_once()


if __name__ == "__main__":
    unittest.main()
