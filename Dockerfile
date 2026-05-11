FROM python:3.10-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install system dependencies
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (layer cache)
COPY pyproject.toml uv.lock /app/

# Install dependencies into system Python (no venv needed in container)
ENV UV_SYSTEM_PYTHON=1
RUN uv sync --frozen --no-dev

# Set Airflow and dbt environment
ENV DBT_PROFILES_DIR=/app/dbt_projects
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags

# Copy application code
COPY . /app

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9090
ENTRYPOINT ["/entrypoint.sh"]
