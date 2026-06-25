# CDC with dlt and DuckDB

Change Data Capture (CDC) pipeline that reads PostgreSQL's Write-Ahead Log using [dlt](https://dlthub.com) and applies changes to a DuckDB analytics table via `MERGE INTO`.

This repository is the companion code for the article *CDC Explained: Why Full Refresh ETL Is Dying*, the third part in a series on the modern single-engineer data stack.

---

## What this does

Instead of truncating and reloading an entire table on each run (full refresh), this pipeline:

1. Takes an **initial snapshot** of the source tables directly from PostgreSQL
2. Streams every subsequent **insert, update, and delete** from the WAL via a replication slot
3. Applies those changes to a DuckDB target table with a single **`MERGE INTO`** statement

The result is a DuckDB table that stays in sync with the source database, with deletes included at near-zero compute overhead compared to a full reload.

---

## Stack

| Layer | Tool |
|---|---|
| Source | PostgreSQL 14+ with `wal_level = logical` |
| Ingestion | [dlt](https://dlthub.com) `pg_replication` verified source |
| Destination | DuckDB 1.5+ |
| Package manager | [uv](https://docs.astral.sh/uv/) |

---

## Prerequisites

- PostgreSQL 14 or later (local or remote)
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) installed
- The PostgreSQL user must have the `REPLICATION` attribute and own the tables being replicated (or be a superuser)

---

## Setup

### 1. Clone the repo and install dependencies

```bash
git clone git@github.com:nenalukic/cdc-dlt-duckdb.git
cd cdc-dlt-duckdb
uv sync
```

`uv sync` installs all dependencies from the lockfile into a local `.venv`. The `pg_replication/` source package is already included in the repo.

### 2. Enable logical replication on PostgreSQL

If you don't have PostgreSQL installed, use Docker. If you already have it running locally, apply the settings with `ALTER SYSTEM`.

For a local Docker instance, pass the flags at startup:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: appdb
    ports:
      - "5432:5432"
    command: >
      postgres
        -c wal_level=logical
        -c max_replication_slots=4
        -c max_wal_senders=4
```

For a self-managed database, run as superuser and restart:

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
```

### 3. Set your credentials

Edit `pipeline.py` and `capture_changes.py` to point at your database:

```python
CREDENTIALS = "postgresql://your_user:your_password@localhost:5432/your_db"
```

---

## Running the pipeline

### Initial snapshot + first change batch

```bash
uv run pipeline.py
```

This creates a replication slot and publication, loads a full snapshot of the source tables into DuckDB staging, then captures any changes that arrived during or after the snapshot.

### Capture ongoing changes

Run this on a schedule (cron, Airflow, etc.) to stream new inserts, updates, and deletes:

```bash
uv run capture_changes.py
```

### Apply changes to the analytics table

```bash
uv run merge.py
```

This reads the latest change batch from `staging_staging` and applies it to the `orders` target table using `MERGE INTO`. Run this after each `capture_changes.py` call.

---

## How dlt stages CDC events

dlt writes change events into two schemas inside `cdc_pipeline.duckdb`:

| Schema | Contents |
|---|---|
| `staging` | dlt's own merged view — deletes are already applied here |
| `staging_staging` | Raw change events, one row per event. **This is what `MERGE INTO` reads from.** |

Each row in `staging_staging` carries:

- `lsn` — the WAL position of the event
- `deleted_ts` — non-null timestamp for delete events, `NULL` for inserts and updates

There is no `op` string column. Delete events are identified by `deleted_ts IS NOT NULL`.

---

## File reference

| File | Purpose |
|---|---|
| `pipeline.py` | Initial snapshot + first CDC batch |
| `capture_changes.py` | Ongoing change capture (run on a schedule) |
| `merge.py` | Apply latest CDC batch to DuckDB target via `MERGE INTO` |
| `code_test_results.md` | Full test log — what each snippet did, errors hit, and fixes applied |

