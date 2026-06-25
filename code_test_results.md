# Code Snippet Test Results

**Tested on:** 2026-06-25  
**Environment:** macOS, Python 3.13.2, dlt 1.28.1, DuckDB 1.5.4, PostgreSQL 14.17

---

## Setup (Snippet 1): `uv init` / `uv add`

**Article code:**
```bash
uv init cdc-pipeline
cd cdc-pipeline
uv add "dlt[duckdb]" duckdb psycopg2-binary
uv run pipeline.py
```

**Result:** First three commands run cleanly. The `uv add` installs all packages successfully.

**Bug:** The `uv add` line is missing a required dependency. `pg_replication` (dlt's verified source for PostgreSQL) internally imports from `dlt.sources.sql_database`, which requires SQLAlchemy. Without it, every import from the `pg_replication` package fails with:

```
dlt.common.exceptions.MissingDependencyException: You must install additional
dependencies to run `dlt sql_database helpers`.
```

**Fix:** Add `dlt[sql_database]` to the install command:
```bash
uv add "dlt[duckdb]" "dlt[sql_database]" duckdb psycopg2-binary
```

---

## Snippet 2: `docker-compose.yml`

**Result:** Not executed (Docker daemon was not running). The YAML syntax is valid and the PostgreSQL configuration parameters (`wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`) are correct. Equivalent settings were applied via `ALTER SYSTEM` instead (see Snippet 3).

---

## Snippet 3: `ALTER SYSTEM` PostgreSQL commands

**Article code:**
```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- Then restart PostgreSQL
```

**Result:** All three commands executed without error. PostgreSQL WAL level confirmed changed from `replica` to `logical` after restart.

```
 wal_level
-----------
 logical
```

**Status: PASS**

---

## Snippet 4: `pipeline.py` (dlt replication source)

**Article code:**
```python
import dlt
from dlt.sources.sql_database.schema_types import table_schema

source = dlt.sources.pg_replication(
    slot_name="cdc_pipeline_slot",
    publication_name="cdc_pipeline_pub",
    schema_name="public",
    table_names=["orders", "customers", "products"],
    credentials="postgresql://postgres:postgres@localhost:5432/appdb",
)

pipeline = dlt.pipeline(
    pipeline_name="cdc_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

load_info = pipeline.run(source)
print(load_info)
```

**Result:** Fails with two distinct errors.

**Error 1 — wrong import (line 2):**
```
dlt.common.exceptions.MissingDependencyException:
You must install additional dependencies to run `dlt sql_database helpers`.
```
`table_schema` lives in `dlt.sources.sql_database`, which requires SQLAlchemy. This import is also unused — nothing in the snippet calls `table_schema`.

**Error 2 — wrong API (line 7):**
```
AttributeError: module 'dlt.sources' has no attribute 'pg_replication'
```
`dlt.sources.pg_replication(...)` does not exist. The `pg_replication` source is a verified source shipped as a local package, not a built-in `dlt.sources` module. It must be scaffolded first with `dlt init pg_replication duckdb`, which generates a `pg_replication/` directory in the project. The actual API uses `init_replication` + `replication_resource`, not a single `dlt.sources.pg_replication(...)` call.

**Fix — corrected pipeline:**
```python
import dlt
from pg_replication import replication_resource
from pg_replication.helpers import init_replication

CREDENTIALS = "postgresql://postgres:postgres@localhost:5432/appdb"
SLOT_NAME = "cdc_pipeline_slot"
PUB_NAME = "cdc_pipeline_pub"

pipeline = dlt.pipeline(
    pipeline_name="cdc_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

# Initial snapshot: captures all existing rows before replication begins
snapshot = init_replication(
    slot_name=SLOT_NAME,
    pub_name=PUB_NAME,
    schema_name="public",
    table_names=["orders", "customers", "products"],
    credentials=CREDENTIALS,
    persist_snapshots=True,
    reset=True,
)
load_info = pipeline.run(snapshot)
print(load_info)

# Ongoing changes: streams inserts, updates, and deletes from the WAL
changes = replication_resource(SLOT_NAME, PUB_NAME, credentials=CREDENTIALS)
load_info = pipeline.run(changes)
print(load_info)
```

**Corrected pipeline output:**
```
Pipeline cdc_pipeline load step completed in 0.42 seconds
1 load package(s) were loaded to destination duckdb and into dataset staging
The duckdb destination used duckdb:////path/to/cdc_pipeline.duckdb location to store data
Load package 1782384549.0518951 is LOADED and contains no failed jobs

Pipeline cdc_pipeline load step completed in 0.18 seconds
1 load package(s) were loaded to destination duckdb and into dataset staging
The duckdb destination used duckdb:////path/to/cdc_pipeline.duckdb location to store data
Load package 1782384549.746856 is LOADED and contains no failed jobs
```

Tables loaded into DuckDB staging (`orders`, `customers`, `products`):
```
staging.orders columns: ['order_id', 'customer_id', 'status', 'amount', 'updated_at', '_dlt_load_id', '_dlt_id']
(1, 100, 'pending', Decimal('49.99'), datetime(...))
(2, 101, 'shipped', Decimal('129.00'), datetime(...))

staging.customers columns: ['customer_id', 'name', 'email', 'created_at', '_dlt_load_id', '_dlt_id']
(100, 'Alice', 'alice@example.com', datetime(...))
(101, 'Bob', 'bob@example.com', datetime(...))

staging.products columns: ['product_id', 'name', 'price', 'updated_at', '_dlt_load_id', '_dlt_id']
(1, 'Widget', Decimal('9.99'), datetime(...))
```

**Status: FAILS as written, PASS after fix**

---

## Snippet 5: DuckDB `MERGE INTO`

**Article code:**
```python
import duckdb

con = duckdb.connect("analytics.duckdb")

con.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id     INTEGER PRIMARY KEY,
        customer_id  INTEGER,
        status       VARCHAR,
        amount       DECIMAL(10,2),
        updated_at   TIMESTAMP,
    );

    MERGE INTO orders AS target
    USING (
        SELECT *
        FROM staging.orders
        WHERE _dlt_load_id = (SELECT MAX(_dlt_load_id) FROM staging.orders)
    ) AS source
        ON target.order_id = source.order_id
    WHEN MATCHED AND source.op = 'd' THEN
        DELETE
    WHEN MATCHED AND source.op IN ('u', 'c') THEN
        UPDATE SET ...
    WHEN NOT MATCHED AND source.op != 'd' THEN
        INSERT ...
""")
```

**Result:** Fails with two distinct errors.

**Error 1 — missing schema:**
```
CatalogException: Table with name "staging.orders" does not exist
because schema "staging" does not exist.
```
`analytics.duckdb` has no `staging` schema. The staging data lives in `cdc_pipeline.duckdb`. The target and staging databases are separate DuckDB files; the script must `ATTACH` the staging database before referencing it.

**Error 2 — wrong column name (`op`):**
The `op` column (`op = 'd'`, `op IN ('u', 'c')`) does not exist in the dlt staging schema. dlt pg_replication uses `deleted_ts` (a timestamp, non-null when the row was deleted) instead of an `op` string. The correct equivalents:

| Article | Actual dlt schema |
|---|---|
| `source.op = 'd'` | `source.deleted_ts IS NOT NULL` |
| `source.op IN ('u', 'c')` | `source.deleted_ts IS NULL` |
| `source.op != 'd'` | `source.deleted_ts IS NULL` |

**Additional note — where dlt stores raw change events:**  
`staging.orders` in the pipeline DuckDB already reflects dlt's own merged view. The raw change events with `deleted_ts` populated for deletes are in `staging_staging.orders`. The MERGE INTO must target `staging_staging.orders` (not `staging.orders`) to capture delete events.

**Note on trailing comma:** DuckDB 1.5.4 accepts a trailing comma before `)` in `CREATE TABLE`. That part of the snippet is fine.

**Fix — corrected MERGE INTO:**
```python
import duckdb

con = duckdb.connect("analytics.duckdb")
con.execute("ATTACH 'cdc_pipeline.duckdb' AS cdc (READ_ONLY)")

con.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id     INTEGER PRIMARY KEY,
        customer_id  INTEGER,
        status       VARCHAR,
        amount       DECIMAL(10,2),
        updated_at   TIMESTAMP,
    );
""")

con.execute("""
    MERGE INTO orders AS target
    USING (
        SELECT *
        FROM cdc.staging_staging.orders
        WHERE _dlt_load_id = (SELECT MAX(_dlt_load_id) FROM cdc.staging_staging.orders)
    ) AS source
        ON target.order_id = source.order_id
    WHEN MATCHED AND source.deleted_ts IS NOT NULL THEN
        DELETE
    WHEN MATCHED AND source.deleted_ts IS NULL THEN
        UPDATE SET
            customer_id = source.customer_id,
            status      = source.status,
            amount      = source.amount,
            updated_at  = source.updated_at
    WHEN NOT MATCHED AND source.deleted_ts IS NULL THEN
        INSERT (order_id, customer_id, status, amount, updated_at)
        VALUES (source.order_id, source.customer_id, source.status,
                source.amount, source.updated_at);
""")

print("CDC events applied.")
con.close()
```

**End-to-end test result** — started with order_id 1 (pending/49.99) and order_id 2 (shipped/129.00) in the target, then applied CDC batch (update order 1, delete order 2, insert order 3):

```
Pre-CDC state:
(1, 100, 'pending',  Decimal('49.99'),  datetime(2026, 6, 25, 10, 0))
(2, 101, 'shipped',  Decimal('129.00'), datetime(2026, 6, 25, 10, 0))

Post-CDC state:
(1, 100, 'delivered', Decimal('55.00'), datetime(2026, 6, 25, 12, 47, ...))
(3, 102, 'pending',   Decimal('75.50'), datetime(2026, 6, 25, 12, 49, ...))
```

Order 1 updated, order 2 deleted, order 3 inserted — all three CDC event types confirmed working.

**Status: FAILS as written, PASS after fix**

---

## Summary

| Snippet | Status | Issue |
|---|---|---|
| Shell setup (`uv init` / `uv add`) | **PASS** (partial) | Missing `dlt[sql_database]` in `uv add` |
| `docker-compose.yml` | **NOT RUN** | Docker daemon not running; YAML is syntactically correct |
| `ALTER SYSTEM` SQL | **PASS** | Runs cleanly; wal_level confirmed changed to `logical` |
| `pipeline.py` (dlt source) | **FAIL** | Wrong import (`table_schema`), wrong API (`dlt.sources.pg_replication` doesn't exist) |
| DuckDB `MERGE INTO` | **FAIL** | Wrong schema reference (`staging.orders` vs `staging_staging.orders`), wrong column name (`op` vs `deleted_ts`) |

