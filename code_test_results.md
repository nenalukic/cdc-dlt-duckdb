# Code Test Results

**Tested on:** 2026-06-25
**Environment:** macOS, Python 3.13.2, dlt 1.28.1, DuckDB 1.5.4, PostgreSQL 14.17

---

## Snippet 1: PostgreSQL setup

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
```

**Result:**

```
ALTER SYSTEM
ALTER SYSTEM
ALTER SYSTEM
```

WAL level confirmed after restart:

```
 wal_level
-----------
 logical
```

---

## Snippet 2: Install dependencies

```bash
git clone git@github.com:nenalukic/cdc-dlt-duckdb.git
cd cdc-dlt-duckdb
uv sync
```

**Result:**

```
Resolved 45 packages in 423ms
Installed 42 packages in 79ms
 + dlt==1.28.1
 + duckdb==1.5.4
 + psycopg2-binary==2.9.12
 + sqlalchemy==2.0.51
 ...
```

---

## Snippet 3: Initial snapshot — `pipeline.py`

```python
import dlt
from pg_replication import replication_resource
from pg_replication.helpers import init_replication

CREDENTIALS = "postgresql://your_user@localhost:5432/your_db"
SLOT_NAME = "cdc_pipeline_slot"
PUB_NAME = "cdc_pipeline_pub"

pipeline = dlt.pipeline(
    pipeline_name="cdc_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

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

changes = replication_resource(SLOT_NAME, PUB_NAME, credentials=CREDENTIALS)
load_info = pipeline.run(changes)
print(load_info)
```

**Result:**

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

Tables loaded into DuckDB staging:

```
staging.orders columns:    ['order_id', 'customer_id', 'status', 'amount', 'updated_at', '_dlt_load_id', '_dlt_id']
staging.customers columns: ['customer_id', 'name', 'email', 'created_at', '_dlt_load_id', '_dlt_id']
staging.products columns:  ['product_id', 'name', 'price', 'updated_at', '_dlt_load_id', '_dlt_id']
```

---

## Snippet 4: Capture ongoing changes — `capture_changes.py`

PostgreSQL changes applied before running:

```sql
UPDATE orders SET status = 'delivered', amount = 55.00 WHERE order_id = 1;
DELETE FROM orders WHERE order_id = 2;
INSERT INTO orders VALUES (3, 102, 'pending', 75.50, NOW());
```

```python
import dlt
from pg_replication import replication_resource

CREDENTIALS = "postgresql://your_user@localhost:5432/your_db"

pipeline = dlt.pipeline(
    pipeline_name="cdc_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

changes = replication_resource("cdc_pipeline_slot", "cdc_pipeline_pub", credentials=CREDENTIALS)
load_info = pipeline.run(changes)
print(load_info)
```

**Result:**

```
Pipeline cdc_pipeline load step completed in 0.30 seconds
1 load package(s) were loaded to destination duckdb and into dataset staging
Load package 1782384595.3516328 is LOADED and contains no failed jobs
```

Change events in `staging_staging.orders` (raw WAL events):

```
columns: ['order_id', 'customer_id', 'status', 'amount', 'updated_at', '_dlt_load_id', '_dlt_id', 'lsn', 'deleted_ts']

(1, 100, 'delivered', 55.00,  ..., lsn=24883992, deleted_ts=None)      # update
(2, 0,   '',          0.00,   ..., lsn=24884096, deleted_ts=2026-06-25) # delete
(3, 102, 'pending',   75.50,  ..., lsn=24884160, deleted_ts=None)      # insert
```

---

## Snippet 5: Apply CDC events — `merge.py`

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

**Result** — target table before and after applying the CDC batch:

```
Before:
(1, 100, 'pending',  49.99,  2026-06-25 10:00:00)
(2, 101, 'shipped',  129.00, 2026-06-25 10:00:00)

After:
(1, 100, 'delivered', 55.00, 2026-06-25 12:47:59)  # updated
(3, 102, 'pending',   75.50, 2026-06-25 12:49:39)  # inserted
```

Order 1 updated, order 2 deleted, order 3 inserted. All three CDC event types confirmed working.
