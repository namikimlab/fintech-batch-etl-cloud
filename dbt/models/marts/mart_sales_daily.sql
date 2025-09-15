{{ config(materialized='table') }}

with base as (
  select
    -- Day (portable)
    {% if target.type == 'redshift' %}
      cast(date_trunc('day', txn_ts) as date) as day
    {% else %}
      cast(strftime('%Y-%m-%d', txn_ts) as date) as day
    {% endif %},
    mcc,
    channel,
    -- Approved-only gross sales (refunds negative)
    sum(
      case when lower(status) = 'approved'
           then case when is_refund then -amount else amount end
           else 0 end
    ) as gross_sales,
    -- Approved-only count
    sum(case when lower(status) = 'approved' then 1 else 0 end) as txn_count
  from {{ ref('fact_transactions') }}
  group by 1,2,3
)
select * from base
