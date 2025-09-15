-- models/staging/stg_transactions.sql
{% if target.type == 'redshift' %}
  {{ config(materialized='view') }}
  select * from {{ source('core', 'stg_transactions') }}
{% else %}
  {{ config(materialized='view') }}
  {% set silver_dir = env_var('SILVER_DIR', '/opt/data/silver') %}
  select
    transaction_id,
    customer_id,
    card_id,
    merchant_id,
    txn_ts,
    amount,
    mcc,
    status,
    is_refund,
    channel
  from read_parquet('{{ silver_dir }}/transactions/**/*.parquet')
  {% if var('process_date', none) is not none %}
    where date_trunc('day', txn_ts) = date '{{ var("process_date") }}'
  {% endif %}
{% endif %}
