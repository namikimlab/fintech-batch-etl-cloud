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
	docker compose exec etl python - <<-'PY'
	import os
	root = os.getenv('SILVER_DIR', '/app/data/silver')
	print('silver_exists=', os.path.isdir(root))
	for dp, _, fn in os.walk(root):
	    if fn:
	        print('partition', dp.replace(root, '').lstrip('/'), 'files', len(fn))
	PY

# One-liner: build → seed → spark → smoke
run:
	make up && make seed && make spark && make smoke
