FROM python:3.10-slim

WORKDIR /app

#Install Poetry
RUN pip install --no-cache-dir poetry

# Update the package list and install Git
RUN apt-get update && apt-get install -y git

#Copy only the dependency files first (for caching efficiency)
COPY pyproject.toml poetry.lock /app/

#Set environnement for dbt profiles directory
ENV DBT_PROFILES_DIR=/app/dbt_projects

#Set environment variables for Airflow (hide absolute paths)
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# Point Airflow to your DAGs
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags  

#Install dependencies (no virtualenv for poetry)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi


#Copy the rest of the application code
COPY . /app


# 8. Copy entrypoint script (for Airflow auto-start)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 9. Expose the default Airflow webserver port
EXPOSE 9090

# 10. Set entrypoint to manage Airflow services
ENTRYPOINT ["/entrypoint.sh"]

