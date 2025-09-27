-- On **Redshift Serverless**: we omit explicit dist/sort keys to allow **AUTO Table Optimization (ATO)** to manage distribution and sort strategy 

-- models/facts/fact_transactions.sql
{% if target.type == 'redshift' %}
  {{ config(
      materialized='incremental',
      unique_key='transaction_id',
      incremental_strategy='merge',
      on_schema_change='sync_all_columns',
      -- Omit dist/sort to allow AUTO (Serverless). 
      post_hook=["analyze {{ this }}"]
  ) }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

with src as (
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
    channel,
    {% if target.type == 'redshift' %}
      cast(to_char(txn_ts, 'YYYYMMDD') as integer) as date_key
    {% else %}
      cast(strftime('%Y%m%d', txn_ts) as integer) as date_key
    {% endif %}
  from {{ ref('stg_transactions') }}
  {% if var('process_date', none) is not none %}
    where date_trunc('day', txn_ts) = date '{{ var("process_date") }}'
  {% endif %}
)
select * from src
