from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

with DAG(
    dag_id="redshift_smoke",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/dags/sql"],
    params={
        "db": Variable.get("REDSHIFT_DB"),
        "workgroup": Variable.get("REDSHIFT_WORKGROUP"),
    },
) as dag:
    run_sql = RedshiftDataOperator(
        task_id="redshift_sql",
        aws_conn_id="aws_default",                # from AIRFLOW_CONN_AWS_DEFAULT
        database="{{ params.db }}",
        workgroup_name="{{ params.workgroup }}",  # for Serverless
        sql="SELECT 1 AS ok;",                       # external file in template_searchpath
    )
