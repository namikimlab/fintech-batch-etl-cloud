# daily_batch_cloud.py
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.models.connection import Connection
from airflow.models import Variable
from scripts.build_copy_sqls import build_copy_sqls as build_copy_sqls_fn  # import function


# --- Environment Variables ---
DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")                          
BRONZE_DIR = os.getenv("BRONZE_DIR", "/app/data/bronze")    
SILVER_DIR = os.getenv("SILVER_DIR", "/app/data/silver")
RECORDS_PER_DAY = os.getenv("RECORDS_PER_DAY", "5000")
DUP_RATE = os.getenv("DUP_RATE", "0.02")
LATE_RATE = os.getenv("LATE_RATE", "0.05")
LOOKBACK_DAYS= os.getenv("LOOKBACK_DAYS", "2")

# --- Airflow Variables ---
S3_BUCKET = Variable.get("S3_BUCKET")
REDSHIFT_DB = Variable.get("REDSHIFT_DB")                  
REDSHIFT_WORKGROUP = Variable.get("REDSHIFT_WORKGROUP")    
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA", "public")    
REDSHIFT_IAM_ROLE_ARN = Variable.get("REDSHIFT_IAM_ROLE_ARN")  # IAM Role for COPY (attached to the Redshift workgroup)
  

default_args = {
    "owner": "nami",
    "retries": 0,
}

# Helper function to build 3 days COPY without error for missing days 
def build_and_push_copy_sqls(**context):
    sql_list = build_copy_sqls_fn(context["ds"])
    return sql_list  # this goes into XCom and can be pulled later

with DAG(
    dag_id="daily_batch_cloud",
    start_date=datetime(2025, 9, 15),
    schedule="0 6 * * *",  # 6AM KST
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    render_template_as_native_obj=True,
    description="M3: seed → spark → S3 → Redshift COPY → dbt (facts incremental + marts)",
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    # 1) Generate one day's raw CSV locally (bronze)
    seed_for_day = BashOperator(
        task_id="seed_for_day",
        bash_command=(
            "python /opt/airflow/scripts/faker_seed.py "
            "--days 1 "
            "--for-date {{ ds }} "
            f"--records-per-day {RECORDS_PER_DAY} "
            f"--dup-rate {DUP_RATE} "
            f"--late-rate {LATE_RATE} "
            "--seed {{ ds_nodash }} "
            f"--bronze-dir {BRONZE_DIR}"
        ),
    )

    # 2) Spark: bronze → S3 silver (Parquet, partitioned by y/m/d), late-aware
    spark_clean_for_day = BashOperator(
        task_id="spark_clean_for_day",
        bash_command=(
            "python /opt/airflow/jobs/clean_transactions.py "
            f"--bronze-dir {BRONZE_DIR} "
            f"--silver-dir {SILVER_DIR} "
            "--process-date {{ ds }} "
            f"--lookback-days {LOOKBACK_DAYS}"
        ),
    )

    # Upload daily silver files to S3 , parquet only 
    sync_silver_to_s3 = BashOperator(
        task_id="sync_silver_to_s3",
        bash_command=(
            "aws s3 sync "
            f"{SILVER_DIR}/transactions/year={{{{ ds_nodash[:4] }}}}/month={{{{ ds_nodash[4:6] }}}}/day={{{{ ds_nodash[6:8] }}}}/ "
            f"s3://{{{{ var.value.S3_BUCKET }}}}/silver/transactions/year={{{{ ds_nodash[:4] }}}}/month={{{{ ds_nodash[4:6] }}}}/day={{{{ ds_nodash[6:8] }}}}/ "
            "--exclude '*' "
            "--include '*.parquet' "
            "--only-show-errors"
        ),
        env={'AWS_CLI_HOME': '/tmp'}, # for cache saving
    )

    # 4) Create Redshift staging table
    create_stg_table = RedshiftDataOperator(
        task_id="create_stg_table",
        aws_conn_id="aws_default",
        database=REDSHIFT_DB,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql="create_stg_transactions.sql",  
    )

    build_copy_sqls = PythonOperator(
        task_id="build_copy_sqls",
        python_callable=build_and_push_copy_sqls,  # <-- call the wrapper
    )

    copy_stg_transactions_3day = RedshiftDataOperator(
        task_id="copy_stg_transactions_3day",
        aws_conn_id="aws_default",
        database=REDSHIFT_DB,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql="{{ ti.xcom_pull(task_ids='build_copy_sqls') }}",  # gets the list
    )

    # 4) dbt: facts (incremental MERGE) — dbt owns the fact table
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/dbt/project && "
            "dbt deps && "
            "dbt run --profiles-dir /opt/dbt/profiles --target redshift "
            "--select path:models/facts "
            "--vars '{process_date: {{ ds }}}'"
        ),
    )

    # 5) dbt: marts (gold)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "set -euo pipefail && "
            "cd /opt/dbt/project && "
            "dbt run --profiles-dir /opt/dbt/profiles --target redshift "
            "--select path:models/marts"
        ),
    )

    # Final task order
    seed_for_day >> spark_clean_for_day >> sync_silver_to_s3 >> create_stg_table >>  build_copy_sqls >> copy_stg_transactions_3day >> dbt_run_facts >> dbt_run_marts

