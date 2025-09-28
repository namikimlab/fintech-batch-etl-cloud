[🇺🇸 English](./README.md) | [🇰🇷 한국어](./README.ko.md)

# 💳 Fintech Batch ETL with Redshift Cloud Integration
> End-to-end batch data pipeline with Airflow, Spark, dbt, Redshift, and S3

[![Spark](https://img.shields.io/badge/Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)]()
[![Postgres](https://img.shields.io/badge/Postgres-336791?style=flat&logo=postgresql&logoColor=white)]()
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)]()
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)]()
[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=flat&logo=metabase&logoColor=white)]()
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)]()
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)]()
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=amazons3&logoColor=white)]()
[![Amazon Redshift](https://img.shields.io/badge/Amazon%20Redshift-8C4FFF?style=flat&logo=amazonredshift&logoColor=white)]()

![workflow](/screenshots/workflow.png)
# Overview
This repository implements a production-style Batch ETL pipeline for processing fintech transaction data.

The pipeline demonstrates how to ingest, clean, transform, and model financial transactions in a way that can processes millions of synthetic transactions daily.

**Use Case**

> Daily credit card transactions are ingested, validated, and stored in a warehouse (Redshift) to support downstream use cases such as **fraud detection, credit risk scoring, and customer segmentation**.

**Key Features**

* **Batch ingestion** of synthetic transaction data (via Faker) into S3 (Bronze layer).
* **Transformations** with Spark to cleanse, deduplicate, and partition data (Silver layer).
* **Warehouse modeling** in Redshift with dbt, including staging, dimension, fact, and mart layers.
* **Data quality checks** with Great Expectations (to be updated) and dbt tests (at warehouse level).
* **Late-arrival handling** for transactions arriving up to 2 days late, ensuring backfill and deduplication logic correctly reconcile delayed data.
* **Cost & performance optimizations** with Parquet/partitioning, Redshift sort/dist keys, and incremental update.
* **Security best practices** with IAM least privilege, S3 encryption, and Secrets Manager for credentials.

# Architecture

## Pipeline Architecture

```
[Faker Generator] 
      │
      ▼
[S3 Bronze (Raw JSON/CSV)]
      │  (cleanse, deduplicate, enrich via Spark)
      ▼
[S3 Silver (Curated Parquet)]
      ├──► [S3 Gold]
      │
      └──► [Redshift Staging] → [Redshift Fact & Dim Tables] → [dbt Marts]
      
[Analytics / BI (Metabase)] consume marts
```

## Components
![Airflow DAG](</screenshots/Screen Shot 2025-09-27 at 12.54.10 PM.png>)

* **Airflow** → Orchestrates all pipeline steps (daily ETL, retries, backfills).
* **Spark** → Cleanses, deduplicates, and partitions raw data into Parquet. Handles schema evolution and late arrivals.
* **AWS S3** →
  * **Bronze**: Raw JSON/CSV dumps, partitioned by `ingest_date`.
  * **Silver**: Curated Parquet with enforced schema and quality checks.
* **AWS Redshift** → Primary warehouse for staging, fact/dim tables, and marts.

* **dbt** → Models marts (RFM, LTV, cohort analysis) and enforces data tests.
* **Great Expectations** → Validates Silver data before load.

## Data Flow: Bronze → Silver → Gold
* **Bronze (Raw Zone)**:

  * Direct landing of Faker-generated transactions in S3.
  * Immutable, schema-on-read, partitioned by ingest date.
  * Retains full raw history for audits and replay.

* **Silver (Curated Zone)**:

  * Spark jobs clean duplicates, enforce schema, and enrich data.
  * Stored as partitioned Parquet (daily + merchant category).
  * Validated with Great Expectations (null checks, enums, range checks).

* **Gold (Business Zone)**:

  * Modeled as star schema (facts + dims).
  * materialized in Redshift via dbt.
  * Used by BI tools (Metabase) for customer and merchant analytics.


# Data Model

## Entities

* **Cards** → issued payment cards linked to customers
* **Merchants** → merchant profiles with category and location
* **Transactions** → credit card transaction events (core fact table)

## Warehouse Layers
![dbt lineage](/screenshots/dbt_graph.png)

* **Staging (`stg_*`)** → 1:1 cleaned data from S3 Silver
* **Dimensions (`dim_*`)** → cards, merchants, customers
* **Facts (`fact_*`)** → transaction-level facts, deduplicated and enriched
* **Marts (`mart_*`)** → business-ready models for RFM, LTV, cohort analysis

# Repository Structure
```bash
.
├── dags/                  # Airflow DAGs for batch ETL
├── data/                  # input data
├── dbt/                   # dbt project (models, schema, profiles)
├── docker/                # Dockerfiles 
├── great_expectations/    # Great Expectations configs and suites
├── jobs/                  # Spark jobs
├── logs/                  # Airflow/Spark/dbt logs
├── scripts/               # helper scripts (seeding, utilities)
├── .env                   # Environment variables
├── docker-compose.yml     # Local orchestration of services
├── Makefile               # Shortcuts for build, run, backfill, tests
├── README.md              # Project documentation
├── requirements.txt       # Python dependencies
```

# Setup & Installation

  * Everything is dockerized.
  * Clone the repo:

    ```bash
    git clone https://github.com/your-username/fintech-batch-etl.git
    cd fintech-batch-etl
    ```

  * Start local services:

    ```bash
    docker compose up -d
    ```

  * Access Airflow at [http://localhost:8080]


# Usage

* **Seeding data** → generate synthetic transactions with `generate transactions.py`.
* **Run pipeline** → trigger the `daily_batch_etl` DAG in Airflow.
* **Build marts** → run `dbt run` to create models in Redshift.
* **Data quality** → dbt test built in dbt run + `run_great_expectations.py`


# Monitoring & Maintenance
![dbt test](/screenshots/dbt_test.png)
* **Airflow UI** → monitor DAG runs, retries, and task logs.
* **dbt docs / lineage** → view model dependencies and test results.


# Future Improvements

Planned improvements to extend the project beyond the current batch ETL pipeline:

* **Data Lakehouse Enhancements**
  * Introduce **Iceberg/Delta tables** on S3 for ACID transactions and upsert support
  * Explore Redshift Spectrum integration for external queries

* **Streaming Pipeline**
  * Add Kafka → Spark Structured Streaming for near real-time ingestion
  * Fraud detection demo with streaming alerts

* **CI/CD & Automation**
  * GitHub Actions for automated dbt runs and tests
  * Linting and unit testing for Spark and Airflow code

* **Infrastructure as Code**
  * Manage AWS resources (S3, Redshift, IAM) with Terraform
  * Parameterize environment setup for reproducibility

* **Monitoring & Observability**
  * Extend OpenLineage integration for dbt models
  * Add alerting on data quality failures (Slack/Email)

* **Cost Optimization** 
  * Evaluate Redshift serveless vs. cluster cost 
  * Use of Redshift Spectrum or Glue + Athena for ad-hoc queires 



---
🪲 by Nami Kim
[Portfolio](https://namikimlab.github.io/) | [GitHub](https://github.com/namikimlab) | [Blog](https://namixkim.com) | [LinkedIn](https://linkedin.com/in/namixkim)