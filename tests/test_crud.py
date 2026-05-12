import unittest
from unittest.mock import AsyncMock, patch

from psql_utils.crud import CRUD as AsyncCRUD
from psql_utils.crud import build_crud as build_async_crud
from psql_utils.crud_sync import CRUD as SyncCRUD
from psql_utils.crud_sync import build_crud as build_sync_crud
from psql_utils.types import t


class CRUDAsyncTests(unittest.IsolatedAsyncioTestCase):

    async def test_save_forwards_defaults_and_call_overrides(self) -> None:
        table = t("users")
        crud = build_async_crud(
            table,
            keys=["name"],
            uniq_keys=["email"],
            json_keys=["meta"],
            save_kwargs={"on_saved": "from-default"},
        )

        with patch("psql_utils.crud.record.save", new_callable=AsyncMock) as m:
            m.return_value = 100
            ret = await crud.save(name="n", ignore_extra_keys=True)

        self.assertEqual(ret, 100)
        m.assert_awaited_once_with(
            table,
            name="n",
            ignore_extra_keys=True,
            keys=["name"],
            uniq_keys=["email"],
            json_keys=["meta"],
            on_saved="from-default",
        )

    async def test_get_forwards_uniq_keys_and_get_kwargs(self) -> None:
        table = t("users")
        crud = AsyncCRUD(
            table,
            uniq_keys=["email"],
            get_kwargs={"required_uniq_keys": False},
        )

        with patch("psql_utils.crud.record.get", new_callable=AsyncMock) as m:
            m.return_value = {"id": 1}
            ret = await crud.get(email="a@x.com", fields=["id"])

        self.assertEqual(ret, {"id": 1})
        m.assert_awaited_once_with(
            table,
            email="a@x.com",
            fields=["id"],
            uniq_keys=["email"],
            required_uniq_keys=False,
        )

    async def test_get_list_and_count_forward_query_kwargs(self) -> None:
        table = t("users")
        crud = AsyncCRUD(
            table,
            json_keys=["meta"],
            query_kwargs={"join_sql": "left join org on org.id=users.org_id"},
        )

        with patch("psql_utils.crud.record.get_list",
                   new_callable=AsyncMock) as m_get_list:
            m_get_list.return_value = [{"id": 1}]
            rows = await crud.get_list(size=10, popup=True)

        self.assertEqual(rows, [{"id": 1}])
        m_get_list.assert_awaited_once_with(
            table,
            size=10,
            json_keys=["meta"],
            join_sql="left join org on org.id=users.org_id",
            popup=True,
        )

        with patch("psql_utils.crud.record.count",
                   new_callable=AsyncMock) as m_count:
            m_count.return_value = 9
            total = await crud.count(active=True)

        self.assertEqual(total, 9)
        m_count.assert_awaited_once_with(
            table,
            active=True,
            json_keys=["meta"],
            join_sql="left join org on org.id=users.org_id",
        )

    async def test_remove_forwards_get_kwargs(self) -> None:
        table = t("users")
        crud = AsyncCRUD(
            table,
            uniq_keys=["email"],
            get_kwargs={"ignore_extra_keys": True},
        )

        with patch("psql_utils.crud.record.remove",
                   new_callable=AsyncMock) as m:
            m.return_value = True
            ret = await crud.remove(email="a@x.com")

        self.assertTrue(ret)
        m.assert_awaited_once_with(
            table,
            email="a@x.com",
            uniq_keys=["email"],
            ignore_extra_keys=True,
        )


class CRUDSyncTests(unittest.TestCase):

    def test_save_forwards_defaults_and_call_overrides(self) -> None:
        table = t("users")
        crud = build_sync_crud(
            table,
            keys=["name"],
            uniq_keys=["email"],
            json_keys=["meta"],
            save_kwargs={"on_saved": "from-default"},
        )

        with patch("psql_utils.crud_sync.record_sync.save") as m:
            m.return_value = 200
            ret = crud.save(name="n", ignore_extra_keys=True)

        self.assertEqual(ret, 200)
        m.assert_called_once_with(
            table,
            name="n",
            ignore_extra_keys=True,
            keys=["name"],
            uniq_keys=["email"],
            json_keys=["meta"],
            on_saved="from-default",
        )

    def test_get_forwards_uniq_keys_and_get_kwargs(self) -> None:
        table = t("users")
        crud = SyncCRUD(
            table,
            uniq_keys=["email"],
            get_kwargs={"required_uniq_keys": False},
        )

        with patch("psql_utils.crud_sync.record_sync.get") as m:
            m.return_value = {"id": 1}
            ret = crud.get(email="a@x.com", fields=["id"])

        self.assertEqual(ret, {"id": 1})
        m.assert_called_once_with(
            table,
            email="a@x.com",
            fields=["id"],
            uniq_keys=["email"],
            required_uniq_keys=False,
        )

    def test_get_list_and_count_forward_query_kwargs(self) -> None:
        table = t("users")
        crud = SyncCRUD(
            table,
            json_keys=["meta"],
            query_kwargs={"join_sql": "left join org on org.id=users.org_id"},
        )

        with patch("psql_utils.crud_sync.record_sync.get_list") as m_get_list:
            m_get_list.return_value = [{"id": 1}]
            rows = crud.get_list(size=10, popup=True)

        self.assertEqual(rows, [{"id": 1}])
        m_get_list.assert_called_once_with(
            table,
            size=10,
            json_keys=["meta"],
            join_sql="left join org on org.id=users.org_id",
            popup=True,
        )

        with patch("psql_utils.crud_sync.record_sync.count") as m_count:
            m_count.return_value = 9
            total = crud.count(active=True)

        self.assertEqual(total, 9)
        m_count.assert_called_once_with(
            table,
            active=True,
            json_keys=["meta"],
            join_sql="left join org on org.id=users.org_id",
        )

    def test_remove_forwards_get_kwargs(self) -> None:
        table = t("users")
        crud = SyncCRUD(
            table,
            uniq_keys=["email"],
            get_kwargs={"ignore_extra_keys": True},
        )

        with patch("psql_utils.crud_sync.record_sync.remove") as m:
            m.return_value = True
            ret = crud.remove(email="a@x.com")

        self.assertTrue(ret)
        m.assert_called_once_with(
            table,
            email="a@x.com",
            uniq_keys=["email"],
            ignore_extra_keys=True,
        )


if __name__ == "__main__":
    unittest.main()
