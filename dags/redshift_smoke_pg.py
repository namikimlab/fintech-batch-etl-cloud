from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="redshift_smoke_pg",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pg_ping = PostgresOperator(
        task_id="pg_ping",
        postgres_conn_id="redshift_default",  # .env의 AIRFLOW_CONN_REDSHIFT_DEFAULT
        sql="select 1 as ok;",
    )
