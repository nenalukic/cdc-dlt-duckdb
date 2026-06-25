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
