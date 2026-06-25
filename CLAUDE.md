# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a blog content repository with companion code. It contains the third article in a trilogy on the modern single-engineer data stack, focused on Change Data Capture (CDC) as a replacement for full refresh ETL pipelines, plus working pipeline scripts that were tested against a live PostgreSQL instance.

**GitHub:** `git@github.com:nenalukic/cdc-dlt-duckdb.git`

## Repository Structure

The article and visuals are gitignored (managed outside version control). Tracked files:

- `README.md` — public-facing project and setup documentation
- `CLAUDE.md` — this file
- `code_test_results.md` — full test log of every code snippet in the article: exact errors hit, root causes, and fixes applied
- `cdc-pipeline/` — working pipeline scripts (gitignored, exists on disk only)

## Series Context

- **Article 1:** DuckDB replacing small-scale Spark
- **Article 2:** Polars replacing Pandas in production pipelines
- **Article 3 (this repo):** CDC replacing nightly full refresh ETL as the ingestion layer

## Visual Placeholder Format

The article uses HTML comment blocks as insertion markers for visuals:

```
<!-- VISUAL N: description — insert here -->
<!-- Subtitle: ... -->
<!-- Caption: ... -->
<!-- Alt text: detailed description of the diagram... -->
```

The three image files (`Visual 1.png`, `Visual 2.png`, `Visual 3.png`) correspond to the three comment blocks in order.

## Verified dlt pg_replication API

The article's code snippets were tested and corrected. The key facts for any future edits:

**Installation:** Both extras are required — `dlt[sql_database]` is needed internally by the replication source even though it's not obvious from the name.
```bash
uv add "dlt[duckdb]" "dlt[sql_database]" duckdb psycopg2-binary
uv run dlt init pg_replication duckdb   # scaffolds pg_replication/ package locally
```

**API:** `dlt.sources.pg_replication()` does not exist. The scaffolded package exposes two functions:
```python
from pg_replication import replication_resource
from pg_replication.helpers import init_replication

snapshot = init_replication(slot_name, pub_name, schema_name, table_names,
                             credentials, persist_snapshots=True, reset=True)
changes = replication_resource(slot_name, pub_name, credentials=credentials)
```

**dlt staging schema structure:** dlt writes into two schemas in the pipeline DuckDB file:
- `staging` — dlt's own merged view; deletes are already applied, deleted rows are absent
- `staging_staging` — raw change events, one row per event, including delete events

The `MERGE INTO` target table must read from `staging_staging`, not `staging`. There is no `op` column. Delete events are identified by `deleted_ts IS NOT NULL` (non-null timestamp); inserts and updates have `deleted_ts = NULL`. The `lsn` column holds the WAL position.

**DuckDB MERGE INTO:** The pipeline DuckDB file and the analytics DuckDB file are separate and must be attached explicitly:
```python
con = duckdb.connect("analytics.duckdb")
con.execute("ATTACH 'cdc_pipeline.duckdb' AS cdc (READ_ONLY)")
# then reference cdc.staging_staging.orders
```

## Technical Stack

The article describes a two-tier CDC stack:
- **Tier 1 (Prototype):** `dlt` `pg_replication` source → DuckDB — pure Python, no Kafka
- **Tier 2 (Production):** Debezium Kafka Connect → Redpanda → DuckDB/Iceberg

Key technical specifics to preserve accurately:
- DuckDB `MERGE INTO` introduced in v1.4.0 LTS (September 2025), extended to Iceberg in v1.5.3 (May 2026)
- Replication slot monitoring: `confirmed_flush_lsn` vs `restart_lsn` drift in `pg_replication_slots`
- PostgreSQL WAL config: `wal_level = logical`, `max_replication_slots = 4`, `max_wal_senders = 4`
