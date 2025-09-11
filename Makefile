ENV ?= .env
include $(ENV)
export

# Build + start the single container
up:
	docker compose up -d --build

# Stop and remove container
down:
	docker compose down -v

# (Optional) nuke local data
clean:
	rm -rf ./data/bronze ./data/silver

# Generate bronze CSVs inside the container
seed:
	docker compose exec etl python scripts/faker_seed.py \
	  --days $(DAYS) --records-per-day $(RECORDS_PER_DAY) \
	  --dup-rate $(DUP_RATE) --late-rate $(LATE_RATE) \
	  --bronze-dir $(BRONZE_DIR)

# Run PySpark job (local mode) inside the container
spark:
	docker compose exec etl python jobs/clean_transactions.py \
	  --bronze-dir $(BRONZE_DIR) --silver-dir $(SILVER_DIR)

# Quick existence check
smoke:
	docker compose exec etl ls -R /app/data/silver/transactions || true

# One-liner: build → seed → spark → smoke
run:
	make up && make seed && make spark && make smoke

airflow-up:
	docker compose up -d --build airflow-webserver airflow-scheduler

airflow-init:
	docker compose exec -T airflow-webserver airflow db init
	docker compose exec -T airflow-webserver airflow users create \
	  --username admin --password admin --firstname Nami --lastname Kim \
	  --role Admin --email you@example.com

airflow-logs:
	docker compose logs -f airflow-scheduler

airflow-trigger:
	docker compose exec -T airflow-webserver airflow dags trigger daily_batch_local

airflow-backfill:
	@if [ -z "$(FROM)" ] || [ -z "$(TO)" ]; then echo "Usage: make airflow-backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 2; fi
	docker compose exec -T airflow-webserver airflow dags backfill daily_batch_local -s $(FROM) -e $(TO)
