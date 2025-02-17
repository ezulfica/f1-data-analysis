#!/bin/sh

echo "Initializing Airflow..."

# Ensure the AIRFLOW_HOME directory exists
mkdir -p "$AIRFLOW_HOME"
mkdir -p /app/dags  # Ensure the DAGs directory exists


# Check if the database is initialized, if not, initialize it
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init
fi

# Start the Airflow scheduler and webserver
echo "Starting Airflow services..."
airflow scheduler &   # Run scheduler in the background
exec airflow webserver  # Run webserver in the foreground
