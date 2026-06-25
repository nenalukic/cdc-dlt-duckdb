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

# staging_staging holds raw change events; staging already has dlt's own merge applied
# deleted_ts is non-null for delete events
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
