# /scripts/build_copy_sqls.py
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

def build_copy_sqls(ds: str):
    """
    Build TRUNCATE + COPY statements only for partitions that exist (D-2, D-1, D).
    Returns a Python list of SQL strings.
    """
    schema = os.getenv("REDSHIFT_SCHEMA", "public")
    bucket = os.getenv("S3_BUCKET")
    base_prefix = "silver/transactions"
    iam_role = os.getenv("REDSHIFT_IAM_ROLE_ARN")

    d = datetime.strptime(ds, "%Y-%m-%d").date()
    candidates = [d - timedelta(days=2), d - timedelta(days=1), d]

    hook = S3Hook(aws_conn_id="aws_default")
    sql_list = [f"TRUNCATE TABLE {schema}.stg_transactions;"]

    for day in candidates:
        year, month, day_ = f"{day.year:04d}", f"{day.month:02d}", f"{day.day:02d}"
        prefix = f"{base_prefix}/year={year}/month={month}/day={day_}/"

        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
        if keys:
            sql_list.append(f"""
            COPY {schema}.stg_transactions
            FROM 's3://{bucket}/{prefix}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET
            """)
        else:
            print(f"[SKIP] No objects under s3://{bucket}/{prefix}")

    return sql_list
