-- /opt/airflow/sql/copy_stg_transactions.sql
TRUNCATE TABLE {{ params.schema }}.stg_transactions;

COPY {{ params.schema }}.stg_transactions
FROM 's3://{{ params.s3_bucket }}/silver/transactions/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:8] }}/'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;

COPY {{ params.schema }}.stg_transactions
FROM 's3://{{ params.s3_bucket }}/silver/transactions/year={{ (execution_date - macros.timedelta(days=1)).strftime("%Y") }}/month={{ (execution_date - macros.timedelta(days=1)).strftime("%m") }}/day={{ (execution_date - macros.timedelta(days=1)).strftime("%d") }}/'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;

COPY {{ params.schema }}.stg_transactions
FROM 's3://{{ params.s3_bucket }}/silver/transactions/year={{ (execution_date - macros.timedelta(days=2)).strftime("%Y") }}/month={{ (execution_date - macros.timedelta(days=2)).strftime("%m") }}/day={{ (execution_date - macros.timedelta(days=2)).strftime("%d") }}/'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;