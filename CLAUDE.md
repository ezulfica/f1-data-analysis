# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Package manager:** `uv` (not pip/poetry). All Python commands run via `uv run`.

```bash
# Run the full pipeline (ingest → dbt)
make pipeline

# Individual steps
make ingest           # fetch F1 dump, validate schema, load to DuckDB
make dbt              # dbt build with local DuckDB target
DBT_TARGET=dev make dbt  # override dbt target

# Tests
uv run pytest tests/ -v

# Linting
uv run ruff check .
uv run ruff format .
```

**Single test:** `uv run pytest tests/test_file.py::test_name -v`

**dbt commands** (`transform/` is at monorepo root, one level up):
```bash
uv run dbt build --project-dir transform --profiles-dir transform --target local
uv run dbt run --select model_name --project-dir transform --profiles-dir transform
```

## Architecture

### Data flow

```
jolpica API (CSV dump)
    ↓ ingestion/pipeline.py  — fetch metadata, hash-check via dlt state, download zip
    ↓ ingestion/schema_validator.py — validate columns against config/contracts/*.yml
    ↓ dlt @dlt.source         — each CSV becomes a table in f1_raw dataset (DuckDB/MotherDuck)
    ↓ transform/              — staging → intermediate → marts
```

### Ingestion F1 (`f1/ingestion/`)

- `main.py` — entry point; reads `f1/config/settings.yaml`, builds pipeline, runs source
- `pipeline.py` — `@dlt.source` function `jolpica_source` and helpers; idempotency via dlt resource state
- `schema_validator.py` — checks CSV columns against YAML contracts in `f1/config/contracts/`; raises `SchemaViolationError` on mismatch
- `models.py` — Pydantic models: `Settings`, `JolpicaSettings`, `DltSettings`, `DumpMetadata`

#### Idempotency pattern

dlt source state is **read-only** inside `@dlt.source` functions. State is written only from `@dlt.resource` functions. To skip re-processing an unchanged dump:
- A sentinel resource `_dump_metadata` (write_disposition=`merge`) stores the last-loaded hash in its resource state
- The source function reads that hash via `source_state().get("resources", {}).get("_dump_metadata", {}).get("hash")` and exits early if it matches
- dlt resets resource state for `write_disposition="replace"` resources before each run; `merge` resources are not reset

### dbt (`transform/`)

Shared across all pipelines (monorepo root). Three-layer model structure: `staging/` → `intermediate/` → `marts/`

Four dbt targets in `transform/profiles.yml`:
| Target | Backend | Use |
|--------|---------|-----|
| `local` | DuckDB file (`f1_jolpica.duckdb`) | default local dev |
| `dev` | MotherDuck `f1_data_dev` | cloud dev |
| `prod` | MotherDuck `f1_data_prod` | production |
| `ci` | DuckDB in-memory | GitHub Actions |

**SQL compatibility:** BigQuery-specific functions (`DATE_DIFF`, `FORMAT_DATE`, `SAFE_DIVIDE`) must be wrapped in cross-adapter macros in `transform/macros/`. Avoid `SELECT * EXCEPT(col)` — not supported in DuckDB.

### Environment / secrets

`.envrc` (racine) charge `f1/config/.env.local` via direnv.

Key env vars:
- `DLT_DESTINATION` — `duckdb` (default) or `motherduck`
- `MOTHERDUCK_TOKEN` — required when `DLT_DESTINATION=motherduck`
- `DUCKDB_PATH` — path to the DuckDB file (default: `f1_jolpica.duckdb`)

### CI

- `f1-ci.yml` — runs pytest on PRs touching `f1/ingestion/**` or `f1/config/contracts/**`
- `f1-dbt-ci.yml` — slim CI: full build on `main`, modified-models-only (`state:modified+`) on PRs touching `transform/**`

### Schema contracts

`f1/config/contracts/*.yml` files declare expected columns per table. Any CSV from jolpica missing a contracted column fails the pipeline before dlt loads anything. Tables without a contract file are loaded as-is.
