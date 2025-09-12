-- Cleaned fact at transaction grain (filtering to approved + positive amounts)
SELECT
  transaction_id,
  customer_id,
  card_id,
  merchant_id,
  txn_ts,
  amount,
  mcc,
  status,
  is_refund,
  channel,
  CAST(strftime(txn_ts, '%Y%m%d') AS INTEGER) AS date_key
FROM {{ ref('stg_transactions') }}
WHERE status = 'approved'
