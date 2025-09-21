CREATE TABLE IF NOT EXISTS public.stg_transactions (
    transaction_id VARCHAR(256),
    customer_id VARCHAR(256),
    card_id VARCHAR(256),
    merchant_id VARCHAR(256),
    txn_ts TIMESTAMP,
    amount DECIMAL(18,2),
    currency VARCHAR(10),
    mcc VARCHAR(10),
    status VARCHAR(50),
    is_refund BOOLEAN,
    channel VARCHAR(50),
    source_file VARCHAR(256),
    ingest_date DATE, 
    ingest_ts TIMESTAMP,
    txn_date DATE
);