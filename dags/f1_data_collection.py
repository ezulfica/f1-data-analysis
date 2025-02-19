from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

# bash_command
cwd = os.getcwd()
poetry_shell_command = f'cd "{cwd}"'
poetry_run_command = f"{poetry_shell_command} && poetry run python -m"
data_prep_command = f"{poetry_run_command} data_prep.main"
data_ingest_command = f"{poetry_run_command} data_ingestion.main"
data_load_command = f"{poetry_run_command} data_load_into_bigquery.main"
dbt_command = f"{poetry_run_command} && dbt build --project-dir dbt_projects"


# Step 2: Define Python function to check output and decide the next step
def check_data_collected():
    """checking if there are file to upload"""
    json_to_prep = list(Path(f"{cwd}/raw").rglob("*.json"))

    # Check if the script printed "SUCCESS"
    if len(json_to_prep) > 0:
        return "data_prep"
    else:
        return "stop_execution"


# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),  # Adjust based on your needs
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    "f1_data_collection",
    default_args=default_args,
    description="Full data pipeline DAG: Ingestion -> Prep -> Load -> dbt",
    schedule_interval=timedelta(days=1),  # Runs daily
    catchup=False,
) as dag:

    # Task 0: Setting up the right folder
    launch_poetry = BashOperator(
        task_id="poetry_launch",
        bash_command=poetry_shell_command,
    )

    # Task 1: Data Ingestion
    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command=data_ingest_command,
    )

    # Condition to stop or not the DAG
    check_data_task = BranchPythonOperator(
        task_id="check_data",
        python_callable=check_data_collected,
    )

    # Task 2 > 1: Stop execution if no files are found
    stop_execution = DummyOperator(task_id="stop_execution")

    # Task 2 > 2: Data prep
    data_prep = BashOperator(
        task_id="data_prep",
        bash_command=data_prep_command,
    )

    # Task 3: Data loading
    data_loading = BashOperator(
        task_id="data_loading",
        bash_command=data_load_command,
    )

    # Task 4: Run dbt models
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=dbt_command,
    )

    # Set dependencies: One step after another
    launch_poetry >> data_ingestion >> check_data_task >> [data_prep, stop_execution]
    data_prep >> data_loading >> run_dbt
