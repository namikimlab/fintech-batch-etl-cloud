"""
PySpark script for cleaning transaction data.

This script reads a rolling window of transaction data from a bronze layer,
deduplicates it based on the latest arrival timestamp, and writes the cleaned
data to a silver layer, partitioned by transaction date.

The process is designed to be idempotent and handles late-arriving data
by reprocessing a configurable lookback window.
"""
import argparse
import os
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F, types as T

# build file paths for lookback days [D-K .. D]
def build_bronze_paths(bronze_dir: str, process_date: str, lookback_days: int) -> list[str]:
    print(f"Building bronze paths for process_date={process_date}, lookback_days={lookback_days}")
    d = datetime.strptime(process_date, "%Y-%m-%d").date()
    dates = [(d - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days, -1, -1)]
    paths = [f"{bronze_dir}/transactions_{ds}.csv" for ds in dates]

    print(f"Dates to include: {dates}")
    print(f"Bronze file paths: {paths}")

    return paths

def read_bronze_window(spark, paths: list[str]):
    df = (spark.read
            .option("header", True)
            .csv(paths))  # Spark accepts a list of files
    print(f"Successfully read bronze window. Row count so far: {df.count()}")
    print(f"Columns: {df.columns}")
    return df

def read_and_normalize_data(spark: SparkSession, bronze_window: DataFrame) -> DataFrame:
    """
    Normalizes a pre-loaded bronze window DataFrame:
      - Trim all strings; empty strings -> NULL
      - Cast: txn_ts -> timestamp, amount -> decimal(18,2), is_refund -> boolean
      - Enforce allowed sets for currency/mcc/status/channel/merchant_id; invalid -> NULL
      - customer_id must match ^CUS\\d{4}$; invalid -> NULL
      - transaction_id/card_id validated as UUID; invalid -> NULL
      - Adds ingest_ts (now) and ingest_date (from path)
    """

    ALLOWED_MCC      = ["5411", "5812", "5999", "4121", "7011"]
    ALLOWED_STATUS   = ["approved", "declined"]
    ALLOWED_CHANNEL  = ["online", "pos", "mobile"]
    ALLOWED_CURRENCY = ["KRW"]
    ALLOWED_MERCHANTS = [f"MER{n}" for n in range(1000, 1021)]  # MER1000..MER1020
    CUS_PATTERN      = r"^CUS\d{4}$"
    UUID_V_PATTERN  = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    TRUE_SET  = ["true", "t", "1", "y", "yes"]
    FALSE_SET = ["false", "f", "0", "n", "no"]

    # Read CSV files and extract 'ingest_date' from the file path
    df = (
        bronze_window
        # keep original file path per-row, then extract ingest_date from name
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "ingest_date",
            F.to_date(
                F.regexp_extract(F.col("source_file"), r"transactions_(\d{4}-\d{2}-\d{2})\.csv", 1),
                "yyyy-MM-dd"
            )
        )
    )

    # Trim all string columns and convert empty strings to NULL
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    for c in str_cols:
        df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))

    # Canonicalize case where useful
    df = (
        df
        .withColumn("status",   F.lower(F.col("status")))
        .withColumn("channel",  F.lower(F.col("channel")))
        .withColumn("currency", F.upper(F.col("currency")))
    )

    # Cast core types
    df = df.withColumn("txn_ts",  F.to_timestamp("txn_ts"))
    df = df.withColumn("amount",  F.col("amount").cast(T.DecimalType(18, 2)))

    # Tolerant boolean for is_refund
    df = df.withColumn(
        "is_refund",
        F.when(F.col("is_refund").isNull(), F.lit(None).cast(T.BooleanType()))
         .otherwise(
            F.when(F.lower(F.col("is_refund")).isin([x.lower() for x in TRUE_SET]),  F.lit(True))
             .when(F.lower(F.col("is_refund")).isin([x.lower() for x in FALSE_SET]), F.lit(False))
             .otherwise(F.lit(None).cast(T.BooleanType()))
         )
    )

    # Enum validations → invalid -> NULL
    df = df.withColumn("currency",  F.when(F.col("currency").isin(ALLOWED_CURRENCY), F.col("currency")).otherwise(F.lit(None)))
    df = df.withColumn("mcc",       F.when(F.col("mcc").isin(ALLOWED_MCC), F.col("mcc")).otherwise(F.lit(None)))
    df = df.withColumn("status",    F.when(F.col("status").isin(ALLOWED_STATUS), F.col("status")).otherwise(F.lit(None)))
    df = df.withColumn("channel",   F.when(F.col("channel").isin(ALLOWED_CHANNEL), F.col("channel")).otherwise(F.lit(None)))
    df = df.withColumn("merchant_id", F.when(F.col("merchant_id").isin(ALLOWED_MERCHANTS), F.col("merchant_id")).otherwise(F.lit(None)))

    # ID validations
    df = df.withColumn("customer_id",   F.when(F.col("customer_id").rlike(CUS_PATTERN), F.col("customer_id")).otherwise(F.lit(None)))
    df = df.withColumn("transaction_id",F.when(F.col("transaction_id").rlike(UUID_V_PATTERN), F.col("transaction_id")).otherwise(F.lit(None)))
    df = df.withColumn("card_id",       F.when(F.col("card_id").rlike(UUID_V_PATTERN), F.col("card_id")).otherwise(F.lit(None)))

    # Derived columns
    df = df.withColumn("ingest_ts", F.current_timestamp())
    df = df.withColumn("txn_date",  F.to_date(F.col("txn_ts")))
    df = df.withColumn("year",  F.date_format("txn_date", "yyyy"))
    df = df.withColumn("month", F.date_format("txn_date", "MM"))
    df = df.withColumn("day",   F.date_format("txn_date", "dd"))

    print(f"Successfully cleaned bronze window. Row count so far: {df.count()}")
    print(f"Columns: {df.columns}")

    return df


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """ Deduplicates transactions based on transaction_id. Record with the most recent ingest timestamp and txn_ts is kept.
    """

    # Define a window to partition by transaction_id and order by arrival and event time.
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("ingest_ts").desc(), F.col("txn_ts").desc()
    )

    # Assign a row number and keep only the first one in each window.
    deduped_df = df.withColumn("rn", F.row_number().over(window)).where("rn = 1").drop("rn")

    total_count = df.count()
    deduped_count = deduped_df.count()
    
    print(f"Deduplication complete: {deduped_count} rows remain ") 
    print(f"({total_count - deduped_count} duplicates removed).")

    return deduped_df


def find_impacted_dates(df: DataFrame) -> Tuple[DataFrame, Optional[List[datetime.date]]]:
    """ Prepares the DataFrame for writing by identifying impacted date partitions."""

    impacted_dates_rows = (
        df.select("txn_date").distinct().where("txn_date IS NOT NULL").collect()
    )
    if not impacted_dates_rows:
        return df, None
    
    impacted_dates = sorted([row["txn_date"] for row in impacted_dates_rows])
    print(f"Impacted partitions: {', '.join(str(d) for d in impacted_dates)}")

    return df, impacted_dates


def write_to_silver(df: DataFrame, silver_dir: str) -> None:
    """ Writes the DataFrame to the silver layer, partitioned by date. """

    output_path = os.path.join(silver_dir, "transactions")
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )

    print(f"Write to silver completed successfully.")

def main() -> None:
    parser = argparse.ArgumentParser(description="Clean transaction data from bronze to silver.")
    parser.add_argument("--bronze-dir", type=str, default="/app/data/bronze", help="Base directory for bronze data (e.g., /app/data/bronze).")
    parser.add_argument("--silver-dir", type=str, default="/app/data/silver", help="Base directory for silver data (e.g., /app/data/silver).")
    parser.add_argument("--process-date", type=str, required=True, help="The date for the job run in YYYY-MM-DD format.")
    parser.add_argument("--lookback-days", type=int, default=2, help="Number of days to look back from the process date to read bronze data.")
    args = parser.parse_args()

    try:
        process_date = datetime.strptime(args.process_date, "%Y-%m-%d")
    except ValueError:
        print("Error: Please provide the date in YYYY-MM-DD format.")
        return
    
    spark = (
        SparkSession.builder.appName("clean-transactions-late-aware")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    bronze_path_list = build_bronze_paths(args.bronze_dir, args.process_date, args.lookback_days)
    
    bronze_window = read_bronze_window(spark, bronze_path_list)

    # Read and normalize the data.
    normalized_df = read_and_normalize_data(spark, bronze_window)

    # Deduplicate transactions.
    deduped_df = deduplicate_transactions(normalized_df)

    # Prepare for writing by identifying impacted partitions.
    to_write_df, impacted_dates = find_impacted_dates(deduped_df)

    if not impacted_dates:
        print("[WARN] No valid txn_date rows after normalization. Nothing to write.")
        spark.stop()
        return

    # 5. Write the cleaned data to the silver layer.
    write_to_silver(to_write_df, args.silver_dir)

    spark.stop()


if __name__ == "__main__":
    
    main()