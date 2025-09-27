# Simplified Makefile for fintech-batch-3

# ==============================================================================
# Docker Commands
# ==============================================================================

up:
	docker compose up -d --build

down:
	docker compose down -v

# ==============================================================================
# Data Generation and Processing
# ==============================================================================

seed:
	docker compose exec etl python scripts/generate_transactions.py

spark-job:
	docker compose exec etl python jobs/clean_transactions.py

# ==============================================================================
# dbt Commands
# ==============================================================================

dbt-run:
	docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt

dbt-test:
	docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt

dbt-docs-generate:
	docker compose exec airflow-webserver dbt docs generate --project-dir /opt/airflow/dbt

dbt-docs-serve:
	docker compose exec airflow-webserver dbt docs serve --project-dir /opt/airflow/dbt --port 8082

# ==============================================================================
# Great Expectations Commands
# ==============================================================================

gx-run:
	docker compose exec etl python scripts/run_great_expectations.py

# ==============================================================================
# Airflow Commands
# ==============================================================================

airflow-up:
	docker compose up -d --build airflow-webserver airflow-scheduler

airflow-init:
	docker compose exec -T airflow-webserver airflow db init
	docker compose exec -T airflow-webserver airflow users create \
	  --username admin --password admin --firstname Admin --lastname User \
	  --role Admin --email admin@example.com

airflow-logs:
	docker compose logs -f airflow-scheduler

airflow-trigger:
	docker compose exec -T airflow-webserver airflow dags trigger daily_batch_local

# ==============================================================================
# Convenience Commands
# ==============================================================================

run-all: up seed spark-job dbt-run dbt-test gx-run

clean:
	rm -rf ./data/duckdb

.PHONY: up down seed spark-job dbt-run dbt-test dbt-docs-generate dbt-docs-serve gx-run airflow-up airflow-init airflow-logs airflow-trigger run-all clean