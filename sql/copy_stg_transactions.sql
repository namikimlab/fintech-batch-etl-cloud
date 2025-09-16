-- Truncate the table to clear old data
TRUNCATE TABLE {{ params.schema }}.stg_transactions;

-- Copy all data from the partitioned S3 directory.
-- Redshift will automatically scan all year=YYYY/month=MM/day=DD sub-folders.
COPY {{ params.schema }}.stg_transactions
FROM 's3://{{ params.s3_bucket }}/silver/transactions/'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET
