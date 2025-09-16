# PySpark transform (local mode). Reads a rolling bronze window -> dedup (newest arrival wins)
# -> writes only impacted txn_date partitions (Parquet) to silver.
# English comments by default.
import argparse, os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", type=str, required=True, help="e.g., /app/data/bronze")
    ap.add_argument("--silver-dir", type=str, required=True, help="e.g., /app/data/silver")
    ap.add_argument("--process-date", type=str, required=True, help="YYYY-MM-DD (logical run date)")
    ap.add_argument("--lookback-days", type=int, default=2, help="Read D, D-1, ..., D-lookback from bronze")
    ap.add_argument("--preview", type=int, default=10, help="How many impacted partitions to print in logs")
    return ap.parse_args()

def build_ingest_paths(bronze_dir, process_date_str, lookback_days):
    d = datetime.strptime(process_date_str, "%Y-%m-%d").date()
    dates = [(d - timedelta(days=i)).isoformat() for i in range(lookback_days, -1, -1)]
    base = os.path.join(bronze_dir, "transactions")
    paths = [os.path.join(base, f"ingest_date={ds}") for ds in dates]
    # Keep only existing folders to avoid Spark errors
    return [p for p in paths if os.path.isdir(p)]

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("clean-transactions-late-aware")
        # Make partition overwrite only touch partitions present in the write DF
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    # 1) Discover a small window of bronze ingest_date folders
    ingest_paths = build_ingest_paths(args.bronze_dir, args.process_date, args.lookback_days)
    if not ingest_paths:
        print(f"[WARN] No bronze folders found for window ending {args.process_date} "
              f"(lookback={args.lookback_days}). Nothing to do.")
        spark.stop()
        return

    print("[INFO] Reading bronze paths:")
    for p in ingest_paths: print("  -", p)

    # 2) Read and normalize + attach 'ingest_date' (arrival signal) from file path
    df = (spark.read.option("header", True).csv(ingest_paths)
          .withColumn(
              "ingest_date",
              F.to_date(F.regexp_extract(F.input_file_name(), r"ingest_date=([0-9\-]+)", 1))
          ))

    # Type normalization
    cleaned = (
        df
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("is_refund", F.when(F.lower("is_refund") == "true", F.lit(True)).otherwise(F.lit(False)))
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("currency", F.upper(F.col("currency")))  # keep KRW normalized
    )

    # 3) Deduplicate: newest arrival (ingest_date DESC) wins; tie-break by txn_ts DESC
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_date").desc(), F.col("txn_ts").desc())
    deduped = cleaned.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

    # 4) Compute impacted txn calendar dates and restrict write to only those partitions
    with_dates = (
        deduped
        .withColumn("year",  F.year("txn_ts"))
        .withColumn("month", F.date_format("txn_ts", "MM"))
        .withColumn("day",   F.date_format("txn_ts", "dd"))
        .withColumn("txn_date", F.to_date("txn_ts"))
    )

    impacted_dates_rows = with_dates.select("txn_date").distinct().where("txn_date IS NOT NULL").collect()
    impacted_dates = sorted([r["txn_date"] for r in impacted_dates_rows])
    if not impacted_dates:
        print("[WARN] No valid txn_date rows after normalization. Nothing to write.")
        spark.stop()
        return

    # Keep only rows we intend to overwrite
    to_write = with_dates.where(F.col("txn_date").isin(impacted_dates))

    # 5) Write only impacted partitions (Parquet), partitioned by business date
    out = os.path.join(args.silver_dir, "transactions")
    (to_write
        .drop("txn_date")  # not needed in the stored schema
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(out)
    )

    # 6) Simple quality metrics
    total = to_write.count()
    distinct_ids = to_write.select("transaction_id").distinct().count()
    dup_rate = float(1 - (distinct_ids / total)) if total else 0.0

    # Log preview of impacted partitions (year-month-day)
    ymd = (
        to_write
        .select("year", "month", "day")
        .distinct()
        .orderBy("year", "month", "day")
        .limit(args.preview)
        .collect()
    )
    impacted_preview = [f"{r['year']}-{r['month']}-{r['day']}" for r in ymd]

    print(f"silver_path={out}")
    print(f"process_date={args.process_date} lookback_days={args.lookback_days}")
    print(f"rows={total} distinct_txn_id={distinct_ids} apparent_dup_rate={dup_rate:.6f}")
    print(f"impacted_partitions_count={len(impacted_dates)}")
    print(f"impacted_partitions_preview={impacted_preview}")

    spark.stop()

if __name__ == "__main__":
    main()
