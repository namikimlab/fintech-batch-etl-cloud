# Datasource (Parquet reader)
# └── Data Asset: "transactions_by_day" (points to directory, not single file)
#     └── Batch Definition: "by_ymd" (tells GX how to pick a slice)
#          └── Batch Request: {year=2025, month=09, day=16} (specific slice)

import great_expectations as gx
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
    BooleanType,
    DateType,
)

my_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("txn_ts", TimestampType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("mcc", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_refund", BooleanType(), True),
    StructField("channel", StringType(), True),
    StructField("ingest_date", DateType(), True),
])

# 1) Load File Data Context
context = gx.get_context(
    mode="file",
    project_root_dir="/opt/airflow/great_expectations",  
)


# 2) Check if datasources exist 
# if not, Add a Spark datasource rooted at the partition tree
try:
    ds = context.data_sources.get("silver")
    print("Datasource 'silver' already exists. Reusing it.")
except LookupError:
    print("Creating new datasource 'silver'")
    ds = context.data_sources.add_spark_filesystem(
        name="silver",
        base_directory="/app/data/silver/transactions",
    )
print(f"✅ Datasource '{ds.name}' registered.")

# 3) Check if asset exist 
# if not, add a Directory Parquet asset: points at the folder that holds all partitions
try:
    asset = ds.get_asset("transactions_dir")
    print("Asset 'transactions_dir' already exists. Reusing it.")
except LookupError:
    print("Creating new directory parquet asset 'transactions_dir'")
    asset = ds.add_directory_parquet_asset(
        name="transactions_dir",
        data_directory=".",                  # relative to the datasource's base_directory
        recursiveFileLookup=True,            # descend into year=/month=/day=/
        pathGlobFilter="**/*.parquet",       # only parquet files
    )

print(f"✅ Asset type {type(asset)} registered.")
print(f"✅ Asset'{asset.name}' registered.")

# 4) Check if batch definition exists, if not, add it
try:
    batch_definition = asset.get_batch_definition("daily")
    print("Batch definition 'daily' already exists. Reusing it.")
except KeyError: 
    print("Creating new batch definition 'daily'")
    batch_definition = asset.add_batch_definition_daily(
        name="daily", 
        column="txn_ts"
    )

print(f"✅ Batch definition'{batch_definition.name}' registered.")
# # Validate 
# print("✅ Validating batch definition.")
# batch = batch_definition.get_batch(batch_parameters={"year": 2025, "month": 9, "day": 16})
# print(batch.head())