DBT_TARGET ?= local

# Pipeline steps (run individually without Airflow)
ingest:
	python -m data_ingestion.main

prep:
	python -m data_prep.main

load:
	python -m data_load_into_bigquery.main

dbt:
	dbt build --project-dir dbt_projects --target $(DBT_TARGET)

pipeline: ingest prep load dbt

# Docker Compose stack
up:
	docker-compose up --build -d

down:
	docker-compose down

logs:
	docker-compose logs -f

# Shortcuts
s3-status:
	curl -s http://localhost:4566/_localstack/health | python3 -m json.tool

.PHONY: ingest prep load dbt pipeline up down logs s3-status
