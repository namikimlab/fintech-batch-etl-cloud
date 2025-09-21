# --- continue after you've created `context`, `ds`, and `asset` exactly as you showed ---

from datetime import datetime
import great_expectations as gx
import great_expectations.expectations as gxe

data_source_name = "silver"
data_asset_name = "transactions"
batch_definition_name = "silver_daily"

# Load context, data source, data asset, batch 
context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)

batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)

batch = batch_definition.get_batch()

# Define the Expectation to test
expectation = gxe.ExpectColumnMaxToBeBetween(
    column="amount", min_value=0, max_value=1000000
)

# Test the Expectation:
validation_results = batch.validate(expectation)
print(validation_results)