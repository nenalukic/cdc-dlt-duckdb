# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a blog content repository, not a software project. It contains the third article in a trilogy on the modern single-engineer data stack, focused on Change Data Capture (CDC) as a replacement for full refresh ETL pipelines.

**Article:** `CDC Explained_ Why Full Refresh ETL Is Dying.md`
**Visuals:** `Visual 1.png`, `Visual 2`, `Visual 3.png`

## Series Context

- **Article 1:** DuckDB replacing small-scale Spark
- **Article 2:** Polars replacing Pandas in production pipelines
- **Article 3 (this repo):** CDC replacing nightly full refresh ETL as the ingestion layer

## Visual Placeholder Format

The article uses HTML comment blocks as insertion markers for visuals. The format is:

```
<!-- VISUAL N: description — insert here -->
<!-- Subtitle: ... -->
<!-- Caption: ... -->
<!-- Alt text: detailed description of the diagram... -->
```

These comments carry the full alt text and caption needed for accessibility and publication. When editing the article, preserve this format — the three image files (`Visual 1.png`, `Visual 2`, `Visual 3.png`) correspond to the three comment blocks in order.

## Technical Stack Covered

The article describes a two-tier CDC stack:
- **Tier 1 (Prototype):** `dlt` `pg_replication` source → DuckDB — pure Python, no Kafka
- **Tier 2 (Production):** Debezium Kafka Connect → Redpanda → DuckDB/Iceberg

Key technical specifics to preserve accurately:
- DuckDB `MERGE INTO` introduced in v1.4.0 LTS (September 2025), extended to Iceberg in v1.5.3 (May 2026)
- Replication slot LSN tracking via `confirmed_flush_lsn` vs `restart_lsn`
- PostgreSQL WAL config: `wal_level = logical`, `max_replication_slots = 4`, `max_wal_senders = 4`
- dlt change event `op` values: `c` (insert), `u` (update), `d` (delete), `r` (snapshot read)
