FROM python:3.11-slim

WORKDIR /app

#Install Poetry
RUN pip install --no-cache-dir poetry

#Copy only the dependency files first (for caching efficiency)
COPY pyproject.toml poetry.lock /app/

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
EXPOSE 8080

# 10. Set entrypoint to manage Airflow services
ENTRYPOINT ["/entrypoint.sh"]

