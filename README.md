# psql_utils

`psql_utils` is a typed PostgreSQL helper library on top of `psycopg` 3, with both async and sync APIs.

It provides three layers:

- Low-level SQL helpers (`insert`, `update`, `select`, `count`, ...)
- Record-oriented helpers (`record.get/save/get_list/remove/count`)
- Reusable CRUD builders (`build_crud`, `build_crud_exports`)

## Installation

```bash
pip install psql_utils
```

From source:

```bash
git clone https://github.com/Lupino/python-psql-utils.git
cd python-psql-utils
pip install .
```

## Package Layout

- `psql_utils`: async low-level database API
- `psql_utils.sync`: sync low-level database API
- `psql_utils.record`: async record helpers
- `psql_utils.record_sync`: sync record helpers
- `psql_utils.crud`: async CRUD wrappers/builders
- `psql_utils.crud_sync`: sync CRUD wrappers/builders
- `psql_utils.types`: SQL node helpers (`t/c/cs/i`)
- `psql_utils.gen`: SQL string generators
- `psql_utils.errors`: domain exceptions

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

    user_id = await pg.insert(
        users,
        [c("name"), c("email")],
        ("Alice", "alice@example.com"),
        c("id"),
        required=True,
    )

    row = await pg.select_one(
        users,
        [c("id"), c("name"), c("email")],
        part_sql="id=%s",
        args=(user_id,),
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

pg.connect({"dsn": "postgresql://user:password@127.0.0.1:5432/mydb"})

users = t("users")
user_id = pg.insert(
    users,
    [c("name"), c("email")],
    ("Bob", "bob@example.com"),
    c("id"),
)
row = pg.select_one(
    users,
    [c("id"), c("name"), c("email")],
    part_sql="id=%s",
    args=(user_id,),
)
print(row)

pg.close()
```

## Connection Model

- `connect(config)` initializes a global singleton connector.
- Pool is opened with `autocommit=True`.
- Async side uses `AsyncConnectionPool`; sync side uses `ConnectionPool`.
- `close()` shuts down the global pool.
- `on_connected(fn)` registers callbacks executed after each successful `connect()`.

You must call `connect()` before using query helpers.

## SQL Node Helpers

```python
from psql_utils.types import t, c, cs, i

users = t("users")
u = users.alias("u")
users_with_org = users.join(t("org"), "org.id = users.org_id")

cols = cs(["id", "name", "email"])
idx = i("email_idx")
```

## Low-Level APIs

Async and sync modules expose the same function names:

- Schema: `create_table`, `add_table_column`, `create_index`, `drop_table`
- Write: `insert`, `insert_or_update`, `update`, `delete`
- Read/Aggregate: `select`, `select_one`, `select_only`, `select_one_only`, `sum`, `count`, `group_count`

Important behavior:

- `select` / `select_one` return dict rows (`dict_row` cursor factory).
- `required=True` on `insert`/`select` raises `QueryResultError` when result constraints are not met.
- `run_with_pool` can reuse a scoped cursor from `with_cursor(...)`.

## Cursor Context Propagation (`with_cursor`)

When you already have a cursor in an upper layer (for example inside a
transaction), propagate it downward with `with_cursor(...)`. Any nested
`@run_with_pool` function will reuse that cursor.
`run_with_pool` also propagates its internally acquired cursor to nested
`@run_with_pool` calls in the same execution chain.
Inside `@run_with_pool` functions, use `get_cursor()` / `sync.get_cursor()`
to access the current cursor when needed.
Use `@run_with_pool(transaction=True)` when the wrapped function should run
inside `with cur.connection.transaction()`.

Async:

```python
import psql_utils as pg

async with conn.cursor() as cur:
    async with pg.with_cursor(cur):
        await pg.insert(...)
        await pg.update(...)
        await some_service_fn(...)  # nested run_with_pool calls reuse cur
```

Sync:

```python
import psql_utils.sync as pg

with conn.cursor() as cur:
    with pg.with_cursor(cur):
        pg.insert(...)
        pg.update(...)
        some_service_fn()  # nested run_with_pool calls reuse cur
```

## Record API

`record` / `record_sync` provide higher-level CRUD-by-data behavior:

- `get`: load one row by `id` or unique keys
- `save`: upsert-like behavior driven by `id` / `uniq_keys`
- `get_list`: list query with filter operators
- `count`: count query using same filter grammar
- `remove`: delete one row after resolving it

Async example:

```python
from psql_utils import record
from psql_utils.types import t

users = t("users")

user_id = await record.save(
    users,
    keys=["name", "created_at", "updated_at"],
    uniq_keys=["email"],
    name="Alice",
    email="alice@example.com",
)

row = await record.get(users, uniq_keys=["email"], email="alice@example.com")

rows = await record.get_list(
    users,
    status="active",
    fields=["id", "name", "email"],
    sorts="id desc",
    size=20,
)

total = await record.count(users, status="active")
removed = await record.remove(users, uniq_keys=["email"], email="alice@example.com")
```

Sync version has the same signatures without `await`:

```python
from psql_utils import record_sync as record
```

## Filter Operator Suffixes

Available suffixes for `record.get_list` / `record.count` filters:

- `_gt`, `_lt`, `_gte`, `_lte`, `_neq`
- `_like`, `_unlike`
- `_in` (list/CSV/subquery)
- `_match`, `_unmatch`
- `_similar`, `_unsimilar`

Example:

```python
rows = await record.get_list(
    t("users"),
    age_gte=18,
    name_like="%ali%",
    id_in=[1, 2, 3],
)
```

## JSON Field Support

Declare JSON columns via `json_keys`, then use dotted paths in fields/sorts/filters.

```python
rows = await record.get_list(
    t("users"),
    json_keys=["data"],
    fields=["id", "data.nick", "data.score.int as score"],
    data.score_gte=100,
    sorts="data.score.int desc",
)
```

For `save`, `json_keys` and `sub_json_keys` support JSON merge behavior; `replace_keys` can disable merge on selected keys.

## CRUD Builders

Use `build_crud` when you want an object with bound config:

```python
from psql_utils.crud import build_crud
from psql_utils.types import t

crud = build_crud(
    t("users"),
    keys=["name", "created_at", "updated_at"],
    uniq_keys=["email"],
    json_keys=["data"],
    get_kwargs={"required_uniq_keys": False},
)

user_id = await crud.save(name="Alice", email="alice@example.com")
```

Use `build_crud_exports` for module-style exports:

```python
crud, save, get, get_list, count, remove = build_crud_exports(
    t("users"),
    keys=["name", "created_at", "updated_at"],
    uniq_keys=["email"],
)
```

Sync variant:

```python
from psql_utils.crud_sync import build_crud, build_crud_exports
```

## Hooks

- `record.save(..., on_saved=callback)`
  - callback signature: `on_saved(old_record, id)`
  - async version accepts coroutine callbacks
- `record.remove(..., on_removed=callback)`
  - callback signature: `on_removed(old_record)`
  - async version accepts coroutine callbacks

## Exceptions

From `psql_utils.errors`:

- `ValidationError`: invalid caller input/query shape
- `QueryResultError`: required result not satisfied
- `RecordNotFoundError`: update target not found
- `UniqueConflictError`: unique key conflict during save

`PGConnectorError` is raised by async/sync connector modules when database is not connected.

## SQL Fragment Safety

Raw SQL fragment params are validated (for example: `part_sql`, `join_sql`, `groups`, `sorts`, `lock_sql`).

`ValueError` is raised if a fragment contains:

- `;`
- `--`
- `/*`
- `*/`
- `\x00`

For `_in` subquery strings, value must start with `SELECT`.

Guideline: keep user input in bound parameters, not in raw fragments.

## Development

Run tests:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```

Type check:

```bash
python -m mypy psql_utils
```

## License

MIT
