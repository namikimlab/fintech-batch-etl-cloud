import pandas as pd
import os

# The path to the directory containing the parquet files
# This is the local path that corresponds to the path inside the container
data_dir = "/app/data/silver/transactions/year=2025/month=09/day=20"
parquet_file = "part-00000-d532510b-6356-4188-b3c9-e542729ae905.c000.snappy.parquet"
file_path = os.path.join(data_dir, parquet_file)

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
else:
    # Read the Parquet file
    df = pd.read_parquet(file_path)

    # Print the column names and their types
    print(df.dtypes)
