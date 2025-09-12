WITH base AS (
  SELECT *
  FROM {{ ref('fact_transactions') }}
),

-- Define successful purchase rows (approved & amount > 0 & not refund)
success AS (
  SELECT *
  FROM base
  WHERE status = 'approved' AND NOT is_refund AND amount > 0
),

-- Recency is based on successful purchases only
recency AS (
  SELECT
    customer_id,
    MAX(CAST(txn_ts AS TIMESTAMP)) AS last_purchase_ts
  FROM success
  GROUP BY 1
),

-- Frequency on successful purchases only
freq AS (
  SELECT
    customer_id,
    COUNT(*) AS frequency
  FROM success
  GROUP BY 1
),

-- Monetary can be net (include refunds) or gross (exclude refunds).
-- Choose ONE and document it. Below = NET (includes refunds as negative).
monet AS (
  SELECT
    customer_id,
    SUM(CASE WHEN is_refund THEN -amount ELSE amount END) AS monetary
  FROM base
  WHERE status = 'approved'
  GROUP BY 1
),

tx AS (
  SELECT
    coalesce(r.customer_id, f.customer_id, m.customer_id) AS customer_id,
    r.last_purchase_ts,
    f.frequency,
    m.monetary
  FROM recency r
  FULL OUTER JOIN freq f USING (customer_id)
  FULL OUTER JOIN monet m USING (customer_id)
  -- If you want to drop customers with no successful purchase at all:
  WHERE r.last_purchase_ts IS NOT NULL
),

scored AS (
  SELECT
    customer_id,
    last_purchase_ts,
    frequency,
    monetary,
    date_diff('day', CAST(last_purchase_ts AS DATE), current_date) AS recency_days,

    -- Recency: smaller is better ⇒ invert NTILE
    (6 - NTILE(5) OVER (ORDER BY date_diff('day', CAST(last_purchase_ts AS DATE), current_date))) AS r_score,

    -- Frequency & Monetary: larger is better
    NTILE(5) OVER (ORDER BY frequency) AS f_score,
    NTILE(5) OVER (ORDER BY monetary)  AS m_score
  FROM tx
)

SELECT
  *,
  (r_score * 100 + f_score * 10 + m_score) AS rfm_code
FROM scored;
