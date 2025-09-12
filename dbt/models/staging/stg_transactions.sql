-- Read parquet partitions from local silver into DuckDB
WITH src AS (
  SELECT *
  FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
)
SELECT
  transaction_id,
  customer_id,
  card_id,
  merchant_id,
  txn_ts,
  amount,
  mcc,
  lower(status) AS status,
  CAST(is_refund AS BOOLEAN) AS is_refund,
  lower(channel) AS channel
FROM src
WHERE txn_ts IS NOT NULL
