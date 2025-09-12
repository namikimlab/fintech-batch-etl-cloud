WITH base AS (
  SELECT
    CAST(strftime(txn_ts, '%Y-%m-%d') AS DATE) AS day,
    mcc,
    channel,
    SUM(CASE WHEN is_refund THEN -amount ELSE amount END) AS gross_sales,
    COUNT(*) AS txn_count
  FROM {{ ref('fact_transactions') }}
  GROUP BY 1,2,3
)
SELECT * FROM base
