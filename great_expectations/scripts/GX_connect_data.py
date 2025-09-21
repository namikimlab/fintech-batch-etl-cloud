# Datasource (Parquet reader)
# └── Data Asset: points to directory, not single file
#     └── Batch Definition: "by_ymd" (tells GX how to pick a slice)
#          └── Batch Request: {year=2025, month=09, day=16} (specific slice)

import great_expectations as gx

data_source_name = "silver"
data_asset_name = "transactions"
batch_definition_name = "silver_daily"
batch_definition_column = "txn_ts"
suite_name = "my_expectation_suite"

# Load File Data Context
context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)

# Delete if existing data source is present 
try:
    context.data_sources.get(data_source_name)
    context.delete_datasource(data_source_name)
except:
    pass 

# Create data source 
data_source = context.data_sources.add_spark_filesystem(
    name=data_source_name,
    base_directory="/app/data/silver",
)
print(f"✅ Datasource '{data_source.name}' registered.")

# Create data asset - partitioned parquet 
directory_parquet_asset = data_source.add_directory_parquet_asset(
    name=data_asset_name,
    data_directory = "./transactions"
)

print(f"✅ Asset type {type(directory_parquet_asset)}")
print(f"✅ Asset '{directory_parquet_asset.name}' registered.")

# Create batch definition - daily
batch_definition = directory_parquet_asset.add_batch_definition_daily(
    name=batch_definition_name, column=batch_definition_column 
)
print("✅ Batch definition created")

# Test batch 
batch = batch_definition.get_batch()  
print("✅ Sample of data from the batch:")
print(batch.head())

