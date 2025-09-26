# --- continue after you've created `context`, `ds`, and `asset` exactly as you showed ---

from datetime import datetime
import great_expectations as gx
import great_expectations.expectations as gxe

# 1) Load 
context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)

ds = context.data_sources.get("silver")
asset = ds.get_asset("transactions_dir")

