# **CDC Explained: Why Full Refresh ETL Is Dying**

*This is the third article in a trilogy on the modern single-engineer data stack. The first covered [DuckDB replacing small-scale Spark](https://medium.com/@yourusername/duckdb-the-death-of-small-scale-spark). The second covered [Polars replacing Pandas in production pipelines](https://pythondataengineering.net/projects/polars-vs-pandas-production-pipelines). This one closes the ingestion layer.*

---

## **The Default Nobody Questions**

There is a pipeline running somewhere right now that truncates a table, reads every row from the source database, and reloads the entire thing. It has been doing this every night for three years. Nobody remembers writing it. Nobody questions it. It works, mostly, and mostly is good enough.

I know this pipeline. I have written versions of it. It is the first thing you reach for because it is simple, it is self-healing, and it requires almost no operational knowledge to maintain. You can explain it to a stakeholder in one sentence: we copy the whole table every night.

The problem appears slowly. The table grows. The window stretches. The source database starts complaining about the read load at 2 AM. Someone adds a retry loop. Someone else adds a timeout. The pipeline that used to take eight minutes now takes ninety. Nothing broke. It just quietly became expensive and fragile.

Full refresh ETL is not wrong. It is carrying assumptions from a different era, when tables had fifty thousand rows and nobody needed data fresher than yesterday morning. The question is whether those assumptions still match the workloads you are actually running.

For most production pipelines above a few million rows, they do not.

If you have not read the first two articles in this series, the short version is this. DuckDB gives you a compute layer that replaces Spark for pipelines under a hundred gigabytes. Polars gives you a transformation layer that replaces pandas for file-based ETL above one gigabyte. CDC is what feeds both of them with data that is actually fresh.

---

## **What Full Refresh Actually Costs**

The costs of full refresh are real and measurable. They do not appear in a single incident report. They accumulate silently across every run.

The comparison below maps the four dimensions where full refresh and log-based CDC diverge most sharply.

\<\!-- VISUAL 1: Comparison table — insert here \--\> \<\!-- Subtitle: Four Dimensions Where Full Refresh And CDC Diverge \--\> \<\!-- Caption: Full refresh versus log-based CDC across compute cost, data freshness, delete capture, and schema evolution handling. \--\> \<\!-- Alt text: Table with four rows and three columns. Headers are Dimension, Full Refresh, and Log-Based CDC. Row one: Compute cost, reprocesses entire dataset every run burning warehouse credits on unchanged rows, reads only changed rows since last offset resulting in near-zero marginal cost at scale. Row two: Data freshness, bounded by batch cadence typically overnight creating hours of lag, sub-10-second latency from commit to downstream availability. Row three: Delete capture, WHERE updated-at queries never surface deleted rows creating invisible data loss, captures hard deletes as explicit delete events with the op equals d field. Row four: Schema evolution, auto-infers schema each run but re-reads everything, propagates additive changes automatically with destructive changes requiring coordination. \--\>

**Compute Cost**

Every full refresh reprocesses data that did not change. Airbyte estimates that teams moving from full refresh to incremental and CDC pipelines commonly cut cloud data warehouse costs by thirty to sixty percent. That is not a performance optimisation. It is the elimination of waste that was invisible because the bill arrived at the end of the month.

A financial sector case study published by Raymond Consulting Group cut ETL runtime from three hours to under one hour after switching from full to incremental loads. Some models saw three to five times speed improvements on the same hardware, without any infrastructure changes.

**Data Freshness**

Full refresh ties your data's freshness to the batch cadence. A nightly job means your downstream consumers work with data that is up to twenty-four hours old. For most reporting use cases, that is acceptable. For anything touching inventory, pricing, fraud signals, or operational dashboards, it is not.

Log-based CDC delivers sub-ten-second latency from database commit to downstream availability. Shopify's CDC platform, built on Debezium and Kafka, achieves p99 latency under ten seconds from MySQL insertion to event availability across more than a hundred shards.

**Delete Capture**

This is the failure mode I have seen cause the most confusion. A timestamp-based incremental job uses a query like `WHERE updated_at > last_run`. A deleted row has no updated timestamp. It is simply gone. The pipeline never sees it. The destination table retains rows that no longer exist in the source, silently, indefinitely.

Log-based CDC captures hard deletes as explicit events with `op = 'd'`. The downstream system knows a row was removed. It can act on that information. Full refresh cannot offer this at all.

**Schema Evolution**

A full refresh re-infers the schema every run, which means new columns appear automatically. That is the one genuine advantage. The cost is that you re-read everything to get it. Log-based CDC propagates additive changes through the event stream without re-reading historical data. Destructive changes, drops, renames, and type changes, require coordination. Neither approach handles schema drift for free, but CDC's approach scales.

---

## **How CDC Works Structurally**

Before any tools, the mechanism matters. I spent a long time treating CDC as a fancy name for "incremental loading." It is structurally different, and understanding why changes how you approach the problem.

The diagram below shows exactly where the two approaches diverge.

\<\!-- VISUAL 2: Architecture contrast diagram — insert here \--\> \<\!-- Subtitle: The Structural Difference Between Full Refresh And Log-Based CDC \--\> \<\!-- Caption: Full table read and truncate-reload cycle on the left versus WAL-based incremental capture on the right. \--\> \<\!-- Alt text: Two-column architecture diagram. Left column shows full refresh pipeline: source database at top with full table scan arrow, truncate target table step, bulk insert all rows step, downstream consumers at bottom. Red annotation marks read load on source database and hours of data inconsistency during reload. Right column shows log-based CDC pipeline: source database at top with WAL logical decoding arrow, replication slot tracking last processed offset, only changed rows streamed to staging, MERGE INTO target table applying inserts and updates and deletes, downstream consumers at bottom. Green annotation marks under one percent CPU overhead and sub-ten-second commit to delivery latency. \--\>

PostgreSQL writes every committed transaction to its Write-Ahead Log before applying changes to data files. This is the durability mechanism that allows crash recovery. When you set `wal_level = logical`, PostgreSQL records the additional row-level metadata needed to decode changes from the WAL, without adding load to the write path.

Debezium and dlt's PostgreSQL replication source both read from this log via a replication slot. The slot tracks the last consumed position as an LSN, a Log Sequence Number, which is a byte offset into the WAL. The database knows exactly which changes have been consumed and retains the necessary WAL segments until the consumer confirms receipt.

Each change event carries an `op` field. The value is `c` for insert, `u` for update, `d` for delete, and `r` for a row read during the initial snapshot. Your destination receives not just the new state of the row but the operation type, the timestamp of the commit, and the position in the log. This is information that a full refresh cannot produce.

**The Three CDC Approaches And Their Trade-Offs**

Not all CDC implementations read the transaction log. The three approaches differ sharply in overhead, capability, and operational complexity.

Log-based CDC reads the WAL directly. It adds under one percent CPU overhead to the source database at twenty thousand writes per second, captures deletes, preserves exact event ordering, and delivers sub-second to single-digit-second latency. It requires database configuration: `wal_level = logical`, a replication slot, and appropriate permissions.

Trigger-based CDC installs database triggers that fire on every write and record changes to a shadow table. It is portable across database types and requires no special configuration. At high write volumes it adds twenty to thirty percent overhead to the write path, which is a real cost on a busy production database.

Query-based CDC polls a high-watermark column like `updated_at`. It is the simplest approach and requires no database changes. It cannot capture deletes, has ten to sixty-second polling latency, adds read load proportional to poll frequency, and silently misses rows where the timestamp was not updated.

The article you are reading is about log-based CDC. It is the standard above roughly ten thousand writes per second, and it is what Shopify, Zalando, and Netflix run in production.

---

## **The Stack That Makes It Practical**

Two years ago, building a Python CDC pipeline meant wiring together Kafka, Debezium Kafka Connect, a schema registry, and a custom consumer. The infrastructure cost was real. I looked at that stack and decided my pipelines were small enough to live with full refresh for a while longer.

That calculation has changed.

The diagram below shows the two tiers available to you today.

\<\!-- VISUAL 3: Pipeline flow diagram — insert here \--\> \<\!-- Subtitle: Two Paths From The Same Source: Prototype And Production \--\> \<\!-- Caption: dlt pg\_replication into DuckDB for single-source pipelines, graduating to Debezium plus Redpanda for multi-consumer production scale. \--\> \<\!-- Alt text: Two-row flow diagram. Top row labeled Tier 1 Prototype shows PostgreSQL WAL source flowing to dlt pg\_replication Python library flowing to DuckDB staging table flowing to MERGE INTO target table. Labels show pure Python no Java no Kafka and Postgres only. Bottom row labeled Tier 2 Production shows PostgreSQL WAL source flowing to Debezium Kafka Connect flowing to Redpanda broker flowing to multiple consumers including DuckDB and Iceberg and downstream services. Labels show multi-consumer durable replay and graduation path when ten or more systems need the same stream. \--\>

**Tier 1: dlt Replication Into DuckDB**

For PostgreSQL sources, dlt ships a verified `pg_replication` source that reads the WAL directly using psycopg2 and PostgreSQL's built-in pgoutput plugin. No Java. No Kafka. No connector configuration files. Pure Python, runnable today with two commands.

This is the path I recommend for teams starting with CDC. It covers the majority of use cases, it is operationally simple, and it grows into Tier 2 without changing your downstream DuckDB MERGE logic.

**Tier 2: Debezium Plus Redpanda**

When you need more than one downstream consumer reading the same change stream, or when you need durable replay, or when your sources extend beyond PostgreSQL, the standard production stack is Debezium Kafka Connect plus a broker. Redpanda is Kafka API-compatible, deploys as a single binary without ZooKeeper or a JVM, and is the simplest path to a Kafka-compatible broker for teams that do not already run Kafka. The downstream consumer still applies changes with DuckDB MERGE INTO, so your pipeline logic stays identical.

The decision rule is concrete: if fewer than ten downstream systems need the same change stream, Tier 1 is simpler and sufficient. Above ten consumers, or when replay and durability become hard requirements, graduate to Tier 2\.

---

## **The Code**

Setup is four commands.

\# Create project and add the stack  
uv init cdc-pipeline  
cd cdc-pipeline  
uv add "dlt\[duckdb\]" "dlt\[sql\_database\]" duckdb psycopg2-binary  
uv run dlt init pg\_replication duckdb  
uv run pipeline.py

**Step 1: Enable Logical Replication On PostgreSQL**

For local development, use Docker with the configuration pre-applied:

\# docker-compose.yml  
services:  
  postgres:  
    image: postgres:16  
    environment:  
      POSTGRES\_USER: postgres  
      POSTGRES\_PASSWORD: postgres  
      POSTGRES\_DB: appdb  
    ports:  
      \- "5432:5432"  
    command: \>  
      postgres  
        \-c wal\_level=logical  
        \-c max\_replication\_slots=4  
        \-c max\_wal\_senders=4

On a self-managed database, run these as a superuser and restart:

ALTER SYSTEM SET wal\_level \= 'logical';  
ALTER SYSTEM SET max\_replication\_slots \= 4;  
ALTER SYSTEM SET max\_wal\_senders \= 4;  
\-- Then restart PostgreSQL

**Step 2: Configure dlt Replication**

\# pipeline.py  
import dlt  
from pg\_replication import replication\_resource  
from pg\_replication.helpers import init\_replication

CREDENTIALS \= "postgresql://postgres:postgres@localhost:5432/appdb"  
SLOT\_NAME \= "cdc\_pipeline\_slot"  
PUB\_NAME \= "cdc\_pipeline\_pub"

pipeline \= dlt.pipeline(  
    pipeline\_name="cdc\_pipeline",  
    destination="duckdb",  
    dataset\_name="staging",  
)

\# Initial snapshot: captures all rows present before replication begins  
snapshot \= init\_replication(  
    slot\_name\=SLOT\_NAME,  
    pub\_name\=PUB\_NAME,  
    schema\_name="public",  
    table\_names\=\["orders", "customers", "products"\],  
    credentials\=CREDENTIALS,  
    persist\_snapshots\=True,  
    reset\=True,  
)  
load\_info \= pipeline.run(snapshot)  
print(load\_info)

\# Ongoing changes: streams inserts, updates, and deletes from the WAL  
\# dlt tracks the last consumed LSN in the replication slot automatically  
changes \= replication\_resource(SLOT\_NAME, PUB\_NAME, credentials\=CREDENTIALS)  
load\_info \= pipeline.run(changes)  
print(load\_info)

Each loaded table in DuckDB carries `_dlt_load_id` and `_dlt_id` metadata columns. Change events in the raw staging layer add `lsn` (the WAL position) and `deleted_ts` (a timestamp, non-null for deleted rows). There is no `op` string column — delete events are identified by a non-null `deleted_ts`.

**Step 3: Apply CDC Events With DuckDB MERGE INTO**

This is the centrepiece. MERGE INTO was introduced in DuckDB v1.4.0 LTS in September 2025 and extended to Iceberg tables in v1.5.3 in May 2026\. If you read the first article in this series, you used a version of this pattern in the Iceberg REST catalog pipeline. The same statement that applied upserts against a lakehouse table there now applies CDC semantics here: upsert the row if it exists or is new, delete it if the source deleted it.

import duckdb

con \= duckdb.connect("analytics.duckdb")

\# Attach the pipeline's DuckDB file to read from its raw staging layer  
con.execute("ATTACH 'cdc\_pipeline.duckdb' AS cdc (READ\_ONLY)")

\# Create target table if it does not exist  
con.execute("""  
    CREATE TABLE IF NOT EXISTS orders (  
        order\_id     INTEGER PRIMARY KEY,  
        customer\_id  INTEGER,  
        status       VARCHAR,  
        amount       DECIMAL(10,2),  
        updated\_at   TIMESTAMP,  
    );  
""")

\# Apply CDC events: upsert updated or inserted rows, remove deleted rows  
\# staging\_staging holds the raw change events; deleted\_ts is non-null for deletes  
con.execute("""  
    MERGE INTO orders AS target  
    USING (  
        SELECT \*  
        FROM cdc.staging\_staging.orders  
        WHERE \_dlt\_load\_id \= (SELECT MAX(\_dlt\_load\_id) FROM cdc.staging\_staging.orders)  
    ) AS source  
        ON target.order\_id \= source.order\_id  
    WHEN MATCHED AND source.deleted\_ts IS NOT NULL THEN  
        DELETE  
    WHEN MATCHED AND source.deleted\_ts IS NULL THEN  
        UPDATE SET  
            customer\_id \= source.customer\_id,  
            status      \= source.status,  
            amount      \= source.amount,  
            updated\_at  \= source.updated\_at  
    WHEN NOT MATCHED AND source.deleted\_ts IS NULL THEN  
        INSERT (order\_id, customer\_id, status, amount, updated\_at)  
        VALUES (source.order\_id, source.customer\_id, source.status,  
                source.amount, source.updated\_at);  
""")

print("CDC events applied.")  
con.close()

What just happened: you read only the rows that changed since the last run, applied inserts, updates, and deletes in a single statement, and wrote the result to a DuckDB table that is immediately queryable. No truncate. No full reload. No maintenance window.

---

## **Where Full Refresh Still Wins**

CDC is not free. I want to be precise about that, because the pattern I have described above has real failure modes that a full refresh does not.

A replication slot that stalls can cause uncontrolled WAL growth on the source database. PostgreSQL retains WAL segments until the slot confirms consumption, so a stuck consumer means the database disk fills up. Zalando encountered this with low-activity databases and had to contribute upstream fixes to Debezium. You need to monitor `pg_replication_slots` and alert on `confirmed_flush_lsn` versus `restart_lsn` drift.

Schema evolution requires coordination. Adding a nullable column is safe. Renaming a column is not. PostgreSQL's logical decoding does not propagate DDL as a separate event, so a rename appears downstream as a drop plus an add. On a full refresh pipeline, the same rename is handled automatically on the next run. On a CDC pipeline, it is a coordinated migration.

With those trade-offs stated clearly, full refresh remains the right choice in four situations. Small reference tables under a few million rows where the reload cost is trivial. Append-only sources like event logs where you only need to add rows, not update or delete them. Sources that do not expose a transaction log, many SaaS APIs and some managed databases fall into this category. And teams without the operational capacity to monitor replication slots and coordinate schema migrations.

Even mature CDC deployments run periodic full refreshes as reconciliation safety nets. The two approaches are not mutually exclusive.

---

## **The Engineer You Are Becoming**

Three articles into this series, the picture is complete.

DuckDB gave you a compute layer that replaces Spark for any pipeline under a hundred gigabytes, without a cluster, without a JVM, and with full Iceberg write support so your output tables are readable by every engine in your stack. Polars gave you a transformation layer that replaces pandas for file-based ETL above one gigabyte, using lazy evaluation, multi-core execution, and Arrow memory to finish in seconds what used to take minutes. CDC gives you an ingestion layer that replaces nightly full refreshes with a continuous, low-overhead stream of exactly what changed, captured in order, with deletes included.

This week, take the oldest and most expensive full refresh job in your stack and ask four questions. How big is the table it reads? How much of the data changes between runs? Does the downstream system ever need to know about deletes? Could you tolerate sub-ten-second latency instead of overnight?

If the answers are large, small fraction, yes, and yes, you have your migration target.

The data engineer who asks these questions before defaulting to the nightly reload is doing something more than optimising a pipeline. They are taking ownership of the hidden costs in their stack, the compute waste, the staleness, the silent delete blindspot, and replacing them with a structural solution that scales without changing.

I should have asked these questions earlier than I did. The infrastructure was simpler than I thought, and the gains were larger than I expected.

Sources: Shopify Engineering, "Change Data Capture at Shopify," 2021\. Zalando Engineering, "Fabric Event Streams," December 2025\. Gunnar Morling, "Five Advantages of Log-Based Change Data Capture," Debezium blog, 2018\. Airbyte, "Incremental Load vs Full Load ETL," 2024\. dlt documentation, "PostgreSQL replication source," 2025\. DuckDB v1.4.0 release post, duckdb.org, September 2025\. DuckDB v1.5.3 Iceberg release post, duckdb.org, May 2026\.

