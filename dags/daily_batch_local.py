from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
BRONZE_DIR = os.getenv("BRONZE_DIR", "/app/data/bronze")
SILVER_DIR = os.getenv("SILVER_DIR", "/app/data/silver")
RECORDS_PER_DAY = os.getenv("RECORDS_PER_DAY", "5000")
DUP_RATE = os.getenv("DUP_RATE", "0.02")
LATE_RATE = os.getenv("LATE_RATE", "0.05")

default_args = {
    "owner": "nami",
    "retries": 1,
}

with DAG(
    dag_id="daily_batch_local",
    start_date=datetime(2025, 9, 1),
    schedule="0 6 * * *",  # 06:00 KST (Airflow timezone set in compose)
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    description="M1: local seed -> spark (no AWS)",
) as dag:

    # Use Airflow logical date {{ ds }} to seed a single partition/day
    seed_for_day = BashOperator(
        task_id="seed_for_day",
        bash_command=(
            "python /opt/airflow/scripts/faker_seed.py "
            "--days 1 "
            "--for-date {{ ds }} "
            f"--records-per-day {RECORDS_PER_DAY} "
            f"--dup-rate {DUP_RATE} "
            f"--late-rate {LATE_RATE} "
            f"--bronze-dir {BRONZE_DIR}"
        ),
    )

    spark_clean_for_day = BashOperator(
        task_id="spark_clean_for_day",
        bash_command=(
            "python /opt/airflow/jobs/clean_transactions.py "
            f"--bronze-dir {BRONZE_DIR} "
            f"--silver-dir {SILVER_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euo pipefail && "  # ensure error is caught
            "cd /opt/airflow/dbt && "
            "dbt deps && " 
            "dbt build --profiles-dir . --target dev"
        ),
    )

    seed_for_day >> spark_clean_for_day >> dbt_run