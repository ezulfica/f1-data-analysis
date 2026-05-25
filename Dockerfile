FROM python:3.10-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock /app/

ENV UV_SYSTEM_PYTHON=1
RUN uv sync --frozen --no-dev

COPY f1/ingestion /app/ingestion
COPY f1/config /app/config

CMD ["python", "-m", "ingestion.main"]
