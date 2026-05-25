# F1 Pipeline

Ingestion pipeline for Formula 1 data from the [Jolpica API](https://github.com/jolpica/jolpica-f1).

## Data flow

```
Jolpica API (CSV dump)
    ↓ schema validation (config/contracts/*.yml)
    ↓ dlt @dlt.source
    ↓ DuckDB (local) / MotherDuck (cloud)
    ↓ transform/models/f1/ (dbt)
```

## Local dev

```bash
# From repo root
make ingest             # fetch dump, validate, load to DuckDB
make dbt                # dbt build (local DuckDB)
make pipeline           # ingest + dbt
DBT_TARGET=dev make dbt # run against MotherDuck dev
```

## Config

- `config/settings.yaml` — Jolpica API + dlt settings
- `config/contracts/*.yml` — expected columns per table (fail fast on schema drift)
- `config/.env.local` — secrets (gitignored), see `.env.example`

## Idempotency

dlt stores the hash of the last loaded dump. If the dump hasn't changed, the pipeline exits early without re-loading.
