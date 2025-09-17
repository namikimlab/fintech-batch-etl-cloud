{% docs marts_overview %}
# Analytics Marts (Gold Layer)

**Scope:** dbt owns **all transformations**. Airflow only orchestrates upstream steps (seed → Spark → S3 silver → Redshift COPY to `stg_transactions`) and then calls dbt for facts+marts.

## Ownership & Contracts

* **Upstream input:** `stg_transactions` (Redshift) populated by Airflow COPY from S3 partitioned Parquet.
* **dbt responsibilities:**

  * Build/merge `fact_transactions` (incremental by day).
  * Build gold marts for reporting.
* **Single writer rule:** Only dbt writes `fact_transactions`. Airflow never merges into facts.

## Business Rules (explicit)

* **Status normalization:** `lower(status)`; only `"approved"` counts as a successful purchase.
* **Refunds:** Represented as `is_refund = true`. When summing sales, refunds are **negative**.
* **Monetary (RFM):** **NET** definition (approved sales minus refunds). If you prefer GROSS, change logic and update this doc.
* **Time grain:** Day-level rollups using warehouse-native date functions.
* **Idempotence:** Facts are incremental with `unique_key = transaction_id` and per-run `process_date`.

## Models

### 1) `fact_transactions`

* **Materialization:**

  * Redshift: `incremental` with `merge`, `unique_key = transaction_id`, `on_schema_change = sync_all_columns`.
  * DuckDB (local dev): `table`.
* **Columns:** `txn_ts`, `amount`, `status`, `is_refund`, `mcc`, `channel`, and derived `date_key` (YYYYMMDD int).
* **Partitioning:** Logical by day via `process_date` filter in the incremental model.

### 2) `mart_sales_daily`

* **Purpose:** Daily sales and transaction counts by `mcc`, `channel`.
* **Filters:** Approved-only rows contribute to metrics. Declined rows do **not** add revenue or counts.
* **Metrics:**

  * `gross_sales` = sum(approved amounts; refunds negative).
  * `txn_count` = count of approved transactions.

### 3) `mart_rfm_customer`

* **Purpose:** Customer-level RFM segmentation.
* **Definitions:**

  * **Recency**: days since last **approved, non-refund, amount > 0** purchase.
  * **Frequency**: count of **approved, non-refund, amount > 0** purchases.
  * **Monetary**: **NET** across **approved** rows: refunds subtract.
* **Scoring:** `NTILE(5)` bins for R/F/M; `r_score` is inverted because “smaller is better”. `rfm_code = r*100 + f*10 + m`.

## Portability Notes

* Models use `target.type` guards so they run on both **Redshift** and **DuckDB**:

  * Day bucketing: Redshift `cast(date_trunc('day', txn_ts) as date)` vs DuckDB `cast(strftime('%Y-%m-%d', txn_ts) as date)`.
  * Date diffs: Redshift `datediff(day, ...)` vs DuckDB `date_diff('day', ...)`.

## Data Quality Tests (dbt)

* **Staging:** `transaction_id` `not_null` + `unique` (severity: warn), `status` `accepted_values` (`approved`,`declined`, case-insensitive).
* **Fact:** `transaction_id` `unique` + `not_null`, `txn_ts` `not_null`, `amount` `not_null`.
* **Freshness (optional):** Source freshness on `stg_transactions` using `txn_ts` or `ingest_ts` if added.

Run:

```bash
dbt test --select stg_transactions+ fact_transactions+ marts
```

## How to Run (Cloud: Redshift)

1. **Facts (incremental for a single day)**

```bash
dbt run --profile fintech_redshift --target dev \
  --select path:models/facts \
  --vars '{process_date: {{ ds }}}'
```

2. **Marts**

```bash
dbt run --profile fintech_redshift --target dev \
  --select path:models/marts
```

## How to Run (Local: DuckDB)

```bash
# Full graph for quick dev
dbt run --profile fintech_duckdb --target dev --select staging+ facts+ marts

# Optional: day-scoped (uses same var)
dbt run --profile fintech_duckdb --target dev \
  --select facts+ --vars '{process_date: 2025-09-01}'
```

## Backfill Strategy

* **Preferred:** Let Airflow backfill days (it will COPY D, D-1, D-2 to staging per run), then dbt runs with `process_date` per logical date.
* **Bulk backfill:** Loop dates in a shell script to call dbt facts with different `process_date` values, then run marts once at the end.

## Edge Cases & Decisions

* **Duplicate `transaction_id`:** If upstream can duplicate IDs, dedupe in staging or adjust `unique_key` to a composite (`transaction_id`, `txn_ts`, `merchant_id`). Current assumption: business-unique `transaction_id` (possible, but unverified—confirm upstream).
* **Negative amounts without `is_refund`:** Treated as valid charges unless `is_refund` is true. If your domain uses negative amounts for refunds, align the logic.
* **Status drift:** We normalize via `lower(status)`. If new statuses appear (e.g., `voided`), add to accepted list and define behavior.

## Suggested Improvements (next steps)

* Add `ingest_ts` and `batch_date` to staging for clearer lineage and fresher freshness checks.
* Add `accepted_values` on `channel` and `mcc` (domain dictionary).
* Add exposures for dashboards that consume `mart_sales_daily` and `mart_rfm_customer`.
* Add Great Expectations or Elementary for richer observability alongside dbt tests.

{% enddocs %}