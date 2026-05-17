# psql_utils

A lightweight PostgreSQL utility library that provides:

- Async and sync connection-pool wrappers based on `psycopg`
- Convenient SQL helpers (`insert / update / delete / select / count / sum`)
- Business-oriented `record` helpers (`get/save/remove/get_list/count`)
- `CRUD` builders to export module-level `save/get/get_list/count/remove`

## Installation

```bash
pip install psql_utils
```

Install from source:

```bash
git clone https://github.com/Lupino/python-psql-utils.git
cd python-psql-utils
python setup.py install
```

## Module Layers

- `psql_utils`: async low-level SQL API (`await` required)
- `psql_utils.sync`: sync low-level SQL API
- `psql_utils.record`: async business record API
- `psql_utils.record_sync`: sync business record API
- `psql_utils.crud`: async CRUD builder
- `psql_utils.crud_sync`: sync CRUD builder
- `psql_utils.types`: SQL helper types (`t/c/cs/i`)

## Quick Start (Async)

```python
import asyncio

import psql_utils as pg
from psql_utils.types import t, c


async def main() -> None:
    await pg.connect({
        "dsn": "postgresql://user:password@127.0.0.1:5432/mydb",
    })

    users = t("users")

    uid = await pg.insert(
        users,
        [c("name"), c("email")],
        ("Alice", "alice@example.com"),
        c("id"),
    )

    row = await pg.select_one(
        users,
        [c("id"), c("name"), c("email")],
        part_sql="id=%s",
        args=(uid,),
    )
    print(row)

    await pg.close()


if __name__ == "__main__":
    asyncio.run(main())
```

## Quick Start (Sync)

```python
import psql_utils.sync as pg
from psql_utils.types import t, c

pg.connect({
    "dsn": "postgresql://user:password@127.0.0.1:5432/mydb",
})

users = t("users")

uid = pg.insert(
    users,
    [c("name"), c("email")],
    ("Bob", "bob@example.com"),
    c("id"),
)

row = pg.select_one(
    users,
    [c("id"), c("name"), c("email")],
    part_sql="id=%s",
    args=(uid,),
)
print(row)

pg.close()
```

## Connection Lifecycle

Async:

```python
import psql_utils as pg

await pg.connect({"dsn": "postgresql://..."})
await pg.close()
```

Sync:

```python
import psql_utils.sync as pg

pg.connect({"dsn": "postgresql://..."})
pg.close()
```

Notes:

- `connect()` uses a global singleton connector.
- Current default is `autocommit=True`.
- Initialize the connection before calling query APIs.

## Helper Types

```python
from psql_utils.types import t, c, cs, i

users = t("users")
name_col = c("name")
cols = cs(["id", "name"])
idx = i("email_idx")
```

Common composition:

- `t("users").alias("u")`
- `t("users").join(t("org"), "org.id = users.org_id")`

## Low-Level SQL API (Async/Sync share names)

Main functions:

- `create_table(table_name, columns)`
- `add_table_column(table_name, columns)`
- `create_index(uniq, table_name, index_name, columns)`
- `insert(table_name, columns, args, ret_column=None, ...)`
- `insert_or_update(table_name, uniq_columns, value_columns=None, ...)`
- `update(table_name, columns, part_sql="", args=())`
- `delete(table_name, part_sql="", args=())`
- `sum(table_name, part_sql="", args=(), column=c("*"), join_sql="")`
- `count(table_name, part_sql="", args=(), column=c("*"), ...)`
- `select(table_name, columns, part_sql="", args=(), ...)`
- `select_one(table_name, columns, part_sql="", args=(), ...)`
- `select_only(table_name, column, part_sql="", args=(), ...)`
- `select_one_only(table_name, column, part_sql="", args=(), ...)`
- `drop_table(table_name)`
- `group_count(table_name, columns, part_sql="", args=(), ...)`

## Record API (Recommended for business logic)

```python
from psql_utils import record
from psql_utils.types import t

users = t("users")

uid = await record.save(
    users,
    keys=["name", "created_at", "updated_at"],
    uniq_keys=["email"],
    name="Alice",
    email="alice@example.com",
)

row = await record.get(
    users,
    uniq_keys=["email"],
    email="alice@example.com",
)

rows = await record.get_list(
    users,
    status="active",
    fields=["id", "name", "email"],
    sorts="id desc",
    size=20,
)

ok = await record.remove(users, uniq_keys=["email"], email="alice@example.com")
```

Sync version:

```python
from psql_utils import record_sync as record
```

Same API, without `await`.

## Query Condition Syntax (`record.get/get_list/count`)

Supported operator suffixes:

- `_gt`: `>`
- `_lt`: `<`
- `_gte`: `>=`
- `_lte`: `<=`
- `_neq`: `!=`
- `_like`: `LIKE`
- `_unlike`: `NOT LIKE`
- `_in`: `IN (...)`
- `_match`: `~*`
- `_unmatch`: `!~*`
- `_similar`: `SIMILAR TO`
- `_unsimilar`: `NOT SIMILAR TO`

Example:

```python
rows = await record.get_list(
    t("users"),
    age_gte=18,
    name_like="%ali%",
    id_in=[1, 2, 3],
)
```

### JSON Field Querying

Declare JSON columns with `json_keys`, then use dotted paths:

```python
rows = await record.get_list(
    t("users"),
    json_keys=["data"],
    fields=["id", "data.nick", "data.score.int as score"],
    data.score_gte=100,
    sorts="data.score.int desc",
)
```

## CRUD Builder

Async:

```python
from psql_utils.crud import build_crud_exports
from psql_utils.types import t

crud, save, get, get_list, count, remove = build_crud_exports(
    t("users"),
    keys=["name", "created_at", "updated_at"],
    uniq_keys=["email"],
    json_keys=["data"],
    get_kwargs={"required_uniq_keys": False},
)
```

Sync:

```python
from psql_utils.crud_sync import build_crud_exports
```

Note:

- `build_crud_exports` now returns explicit callable types (not an `Any` tuple).

## Hooks

`record.save`:

- `on_saved(old_record, id)` runs after save succeeds
- async version accepts coroutine callbacks

`record.remove`:

- `on_removed(old_record)` runs after delete succeeds
- async version accepts coroutine callbacks

## SQL Fragment Safety Rules

The library allows raw SQL fragments in advanced parameters:

- `part_sql`
- `join_sql`
- `groups`
- `sorts`
- `lock_sql`
- `*_in="select ..."` subquery filters

To reduce accidental injection risks, these fragments are validated.
`ValueError` is raised when a fragment contains:

- `;`
- `--`
- `/*`
- `*/`
- `\x00`

For `*_in` subquery strings, the value must also start with `SELECT`.

Important:

- Regular conditions still use parameter binding (`%s`).
- Raw SQL fragments must come from trusted server-side code.
- Never pass user input directly into raw SQL fragments.

## Tests

```bash
python -m unittest discover -s tests -p 'test_*.py'
python -m mypy psql_utils
```

## License

MIT
