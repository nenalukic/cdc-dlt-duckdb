import dlt
from pg_replication import replication_resource

CREDENTIALS = "postgresql://nevenkalukic@localhost:5432/appdb"

pipeline = dlt.pipeline(
    pipeline_name="cdc_pipeline",
    destination="duckdb",
    dataset_name="staging",
)

changes = replication_resource("cdc_pipeline_slot", "cdc_pipeline_pub", credentials=CREDENTIALS)
load_info = pipeline.run(changes)
print(load_info)
