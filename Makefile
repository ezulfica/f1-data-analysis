DBT_TARGET ?= local

# Infrastructure locale
up:
	docker compose up -d

down:
	docker compose down

# Pipeline F1
ingest:
	cd f1 && uv run python -m ingestion.main

dbt:
	uv run dbt build \
		--project-dir transform \
		--profiles-dir transform \
		--target $(DBT_TARGET)

pipeline: ingest dbt

# Qualité
test:
	.venv/bin/python -m pytest tests/ -v

lint:
	.venv/bin/python -m ruff check .
	.venv/bin/python -m ruff format --check .

.PHONY: up down ingest dbt pipeline test lint
