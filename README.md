# aws-data-pipelines

Personal data platform on AWS. One repo, multiple data pipelines, shared dbt transform layer.

## Structure

```
aws-data-pipelines/
  f1/                   # F1 data pipeline (Jolpica → DuckDB/MotherDuck)
    ingestion/          # dlt pipeline
    config/             # settings.yaml + schema contracts
  transform/            # dbt models — all sources (staging → intermediate → marts)
  tests/
    f1/                 # ingestion tests
  setup/                # IAM policies (ECR, OIDC)
  docker-compose.yml    # local infra (floci S3 emulation)
```

## Quick start

```bash
uv sync --group dev     # install deps
make up                 # start local infra (floci)
make pipeline           # ingest F1 data + dbt build
make test               # run all tests
```

## Pipelines

| Pipeline | Source | Image ECR |
|---|---|---|
| F1 | Jolpica API (CSV dump) | `f1-pipeline:latest` |

## Adding a new pipeline

1. Create `<name>/ingestion/` with a dlt source
2. Add `<name>/` to `.github/workflows/<name>-cd.yml`
3. Add models to `transform/models/<name>/` with `+tags: ["<name>"]`
4. Wire up the DAG in the Airflow repo
