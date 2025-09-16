TRUNCATE TABLE {{ params.schema }}.stg_transactions;

COPY {{ params.schema }}.stg_transactions
FROM 's3://{{ params.bucket }}/silver/transactions/year={{ params.year }}/month={{ params.month }}/day={{ params.day }}/'
IAM_ROLE '{{ params.iam_role_arn }}'
FORMAT AS PARQUET
STATUPDATE ON
COMPUPDATE ON;