# Datasource (Parquet reader)
# └── Data Asset: points to directory, not single file
#     └── Batch Definition: "by_ymd" (tells GX how to pick a slice)
#          └── Batch Request: {year=2025, month=09, day=16} (specific slice)

import great_expectations as gx

# Load File Data Context
context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)

# Delete if existing data source is present 
try:
    context.data_sources.get("silver")
    context.delete_datasource("silver")
except:
    pass 

# Create data source 
my_ds = context.data_sources.add_spark_filesystem(
    name="silver",
    base_directory="/app/data/silver",
)
print(f"✅ Datasource '{my_ds.name}' registered.")

# Create data asset - partitioned parquet 
my_asset = my_ds.add_directory_parquet_asset(
    name="transactions",
    data_directory = "./transactions"
)
print(f"✅ Asset type {type(my_asset)}")
print(f"✅ Asset '{my_asset.name}' registered.")

# Create batch definition - daily
batch_definition = my_asset.add_batch_definition_daily(
    name="silver_daily", column="txn_ts"
)
print("✅ Batch definition created")

# Test batch 
batch = batch_definition.get_batch()  
print("✅ Sample of data from the batch:")
print(batch.head())