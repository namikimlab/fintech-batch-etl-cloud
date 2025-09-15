{{ config(materialized='table') }}

with base as (
  select
    transaction_id,
    customer_id,
    card_id,
    merchant_id,
    txn_ts,
    amount,
    mcc,
    lower(status) as status,   -- normalize for robustness
    is_refund,
    channel
  from {{ ref('fact_transactions') }}
),

-- Successful purchases = approved, not refund, positive amount
success as (
  select *
  from base
  where status = 'approved' and not is_refund and amount > 0
),

-- Recency uses successful purchases only
recency as (
  select
    customer_id,
    max(cast(txn_ts as timestamp)) as last_purchase_ts
  from success
  group by 1
),

-- Frequency uses successful purchases only
freq as (
  select
    customer_id,
    count(*) as frequency
  from success
  group by 1
),

-- Monetary = NET (refunds negative) over all approved rows
monet as (
  select
    customer_id,
    sum(case when is_refund then -amount else amount end) as monetary
  from base
  where status = 'approved'
  group by 1
),

tx as (
  select
    coalesce(r.customer_id, f.customer_id, m.customer_id) as customer_id,
    r.last_purchase_ts,
    f.frequency,
    m.monetary
  from recency r
  full outer join freq f using (customer_id)
  full outer join monet m using (customer_id)
  where r.last_purchase_ts is not null  -- drop customers with no successful purchases
),

scored as (
  select
    customer_id,
    last_purchase_ts,
    frequency,
    monetary,

    -- Recency in days (portable)
    {% if target.type == 'redshift' %}
      datediff(day, cast(last_purchase_ts as date), current_date) as recency_days
    {% else %}
      date_diff('day', cast(last_purchase_ts as date), current_date) as recency_days
    {% endif %},

    -- R: smaller is better → invert NTILE
    (6 - ntile(5) over (
       order by
         {% if target.type == 'redshift' %}
           datediff(day, cast(last_purchase_ts as date), current_date)
         {% else %}
           date_diff('day', cast(last_purchase_ts as date), current_date)
         {% endif %}
     )) as r_score,

    -- F & M: larger is better
    ntile(5) over (order by frequency) as f_score,
    ntile(5) over (order by monetary)  as m_score
  from tx
)

select
  *,
  (r_score * 100 + f_score * 10 + m_score) as rfm_code
from scored
