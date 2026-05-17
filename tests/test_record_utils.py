import json
import unittest
from typing import Any

from psql_utils.record_utils import (
    gen_query,
    guess_type,
    prepare_get_list,
    prepare_get_by_uniq,
    prepare_save,
)


class RecordUtilsTests(unittest.TestCase):

    def test_prepare_save_sub_json_missing_old_key_does_not_crash(
            self) -> None:
        keys, args = prepare_save(
            sub_json_keys=["meta"],
            old_record={"id": 1},
            meta={"a": 1},
        )
        self.assertEqual(keys, ["meta"])
        self.assertEqual(args, [json.dumps({"a": 1})])

    def test_prepare_save_sub_json_merges_existing_old_data(self) -> None:
        keys, args = prepare_save(
            sub_json_keys=["meta"],
            old_record={
                "id": 1,
                "meta": {
                    "a": 1,
                    "b": {
                        "x": 1
                    }
                }
            },
            meta={
                "b": {
                    "y": 2
                },
                "c": 3
            },
        )
        self.assertEqual(keys, ["meta"])
        self.assertEqual(
            json.loads(args[0]),
            {
                "a": 1,
                "b": {
                    "x": 1,
                    "y": 2
                },
                "c": 3
            },
        )

    def test_guess_type_does_not_treat_non_numeric_text_as_number(
            self) -> None:
        self.assertEqual(guess_type("1a2"), "")
        self.assertEqual(guess_type("12.34"), "float")
        self.assertEqual(guess_type("1234"), "int")

    def test_gen_query_in_subquery_formats_plain_key(self) -> None:
        sql, args = gen_query(user_id_in="select id from users")
        self.assertEqual(sql, "user_id in (select id from users)")
        self.assertEqual(args, ())

    def test_gen_query_in_subquery_formats_json_path_key(self) -> None:
        query_data: Any = {"data.user_id_in": "select id from users"}
        sql, args = gen_query(
            json_keys=["data"],
            **query_data,
        )
        self.assertEqual(sql, "data#>>'{user_id}' in (select id from users)")
        self.assertEqual(args, ())

    def test_gen_query_in_list_suffix_uses_base_key(self) -> None:
        sql, args = gen_query(id_in=[1, 2, 3])
        self.assertEqual(sql, "id in (%s, %s, %s)")
        self.assertEqual(args, (1, 2, 3))

    def test_gen_query_in_list_suffix_uses_formatted_json_path_key(
            self) -> None:
        query_data: Any = {"data.user_id_in": [1, 2]}
        sql, args = gen_query(
            json_keys=["data"],
            **query_data,
        )
        self.assertEqual(sql, "cast(data#>>'{user_id}' as int) in (%s, %s)")
        self.assertEqual(args, (1, 2))

    def test_gen_query_in_subquery_rejects_unsafe_tokens(self) -> None:
        with self.assertRaises(ValueError):
            gen_query(user_id_in="select id from users; drop table users")

    def test_prepare_get_list_rejects_unsafe_sorts(self) -> None:
        with self.assertRaises(ValueError):
            prepare_get_list(sorts="id desc; drop table users")

    def test_prepare_get_by_uniq_omits_missing_optional_keys(self) -> None:
        get_max_id, props = prepare_get_by_uniq(
            uniq_keys=["tenant_id", "email"],
            optional_keys=["email"],
            required_uniq_keys=True,
            tenant_id=10,
        )
        self.assertTrue(get_max_id)
        self.assertEqual(props["part_sql"], "tenant_id=%s")
        self.assertEqual(props["args"], [10])


if __name__ == "__main__":
    unittest.main()
