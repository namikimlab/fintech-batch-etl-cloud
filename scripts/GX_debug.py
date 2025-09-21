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

print(f"✅ Asset type {type(asset)}")
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


target = context.data_sources
print(f"Available  methods from {target}")
methods = [method for method in dir(target)]
for method in methods:
    print(f"  - {method}")

# print(f"Batch schema: {batch_definition.schema}")

# # Validate 
# print("✅ Validating batch definition.")
# batch = batch_definition.get_batch()
# print(batch.head())


# # Let's examine what the batch definition knows about your data
# print("=== Batch Definition Debug Info ===")

# # 1. Check the partitioner (this is key for daily batch definitions)
# print(f"Partitioner: {batch_definition.partitioner}")
# print(f"Partitioner type: {type(batch_definition.partitioner)}")

# # 2. Get available batch identifiers
# print("\n=== Available Batch Identifiers ===")
# try:
#     batch_identifiers = batch_definition.get_batch_identifiers_list()
#     print(f"Total available batches: {len(batch_identifiers)}")
#     print(f"First 10 batch identifiers: {batch_identifiers[:10]}")
#     print(f"Last 10 batch identifiers: {batch_identifiers[-10:]}")
# except Exception as e:
#     print(f"❌ Failed to get batch identifiers: {e}")

# # 3. Check identifier bundle
# print(f"\n=== Identifier Bundle ===")
# try:
#     print(f"Identifier bundle: {batch_definition.identifier_bundle}")
# except Exception as e:
#     print(f"❌ Failed to get identifier bundle: {e}")

# # 4. Try to build a batch request for the first available batch
# print(f"\n=== Building Batch Request ===")
# try:
#     if 'batch_identifiers' in locals() and batch_identifiers:
#         first_batch_id = batch_identifiers[0]
#         print(f"First batch identifier: {first_batch_id}")
        
#         # Build batch request
#         batch_request = batch_definition.build_batch_request(batch_identifiers=first_batch_id)
#         print(f"Batch request: {batch_request}")
        
#         # Now try to get the actual batch
#         print(f"\n=== Getting Batch ===")
#         batch = batch_definition.get_batch(batch_identifiers=first_batch_id)
#         print("✅ Successfully got batch!")
#         print(f"Batch type: {type(batch)}")
#         print(batch.head())
        
#     else:
#         print("No batch identifiers available to test with")
        
# except Exception as e:
#     print(f"❌ Failed to get batch: {e}")
#     print(f"Exception type: {type(e)}")
    
#     # Let's see if we can get more details about the error
#     import traceback
#     print("\nFull traceback:")
#     traceback.print_exc()


# # Check the actual file structure
# print("\n=== Directory Structure ===")
# import os

# def explore_directory(path, level=0, max_level=4):
#     if level > max_level:
#         return
#     try:
#         items = sorted(os.listdir(path))
#         for item in items[:20]:  # Show more items
#             item_path = os.path.join(path, item)
#             indent = "  " * level
#             if os.path.isdir(item_path):
#                 print(f"{indent}{item}/")
#                 if level < 2:  # Only go 2 levels deep initially
#                     explore_directory(item_path, level + 1, max_level)
#             else:
#                 try:
#                     size = os.path.getsize(item_path)
#                     print(f"{indent}{item} ({size:,} bytes)")
#                 except:
#                     print(f"{indent}{item} [size unknown]")
#     except Exception as e:
#         print(f"{indent}[Error reading directory: {e}]")

# base_path = "/app/data/silver/transactions"
# explore_directory(base_path)