# PySpark transform (local mode). Reads bronze CSV → dedup → writes partitioned Parquet to silver.
import argparse, os
from pyspark.sql import SparkSession, functions as F, Window

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", type=str, required=True)
    ap.add_argument("--silver-dir", type=str, required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("clean-transactions-m0-docker")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    bronze_path = os.path.join(args.bronze_dir, "transactions")
    df = spark.read.option("header", True).csv(bronze_path)

    # Normalize/Type cast
    df = (df
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("is_refund", F.when(F.lower("is_refund")=="true", F.lit(True)).otherwise(F.lit(False)))
        .withColumn("status", F.lower("status"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # Deduplicate by transaction_id (latest wins)
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_ts").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn")==1).drop("rn")

    # Partition columns
    df = (df
        .withColumn("year", F.year("txn_ts"))
        .withColumn("month", F.date_format("txn_ts", "MM"))
        .withColumn("day", F.date_format("txn_ts", "dd"))
    )

    out = os.path.join(args.silver_dir, "transactions")
    (df.write.mode("overwrite").partitionBy("year","month","day").parquet(out))

    # Simple quality printouts
    total = df.count()
    distinct_ids = df.select("transaction_id").distinct().count()
    dup_rate = 1 - (distinct_ids / total) if total else 0.0

    print(f"silver_path={out}")
    print(f"rows={total} distinct_txn_id={distinct_ids} apparent_dup_rate={dup_rate:.6f}")

    spark.stop()

if __name__ == "__main__":
    main()