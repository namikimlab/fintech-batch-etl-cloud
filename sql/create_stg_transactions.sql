CREATE TABLE IF NOT EXISTS {{ params.schema }}.stg_transactions (
  transaction_id VARCHAR(64),
  customer_id    VARCHAR(64),
  card_id        VARCHAR(64),
  merchant_id    VARCHAR(64),
  txn_ts         TIMESTAMP,
  amount         DECIMAL(18,2),
  mcc            VARCHAR(8),
  status         VARCHAR(16),
  is_refund      BOOLEAN,
  channel        VARCHAR(16)
);
