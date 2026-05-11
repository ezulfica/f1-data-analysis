from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

cwd = os.getcwd()
run = f'cd "{cwd}" && python -m'
data_ingest_command = f"{run} data_ingestion.main"
data_prep_command = f"{run} data_prep.main"
data_load_command = f"{run} data_load_into_bigquery.main"
dbt_target = os.environ.get("DBT_TARGET", "dev")
dbt_command = f'cd "{cwd}" && dbt build --project-dir dbt_projects --target {dbt_target}'


def check_data_collected():
    """checking if there are file to upload"""
    json_to_prep = list(Path(f"{cwd}/raw").rglob("*.json"))
    if len(json_to_prep) > 0:
        return "data_prep"
    else:
        return "stop_execution"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 25, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "f1_data_collection",
    default_args=default_args,
    description="Full data pipeline DAG: Ingestion -> Prep -> Load -> dbt",
    schedule_interval=timedelta(days=7),
    catchup=False,
) as dag:

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command=data_ingest_command,
    )

    check_data_task = BranchPythonOperator(
        task_id="check_data",
        python_callable=check_data_collected,
    )

    stop_execution = DummyOperator(task_id="stop_execution")

    data_prep = BashOperator(
        task_id="data_prep",
        bash_command=data_prep_command,
    )

    data_loading = BashOperator(
        task_id="data_loading",
        bash_command=data_load_command,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=dbt_command,
    )

    data_ingestion >> check_data_task >> [data_prep, stop_execution]
    data_prep >> data_loading >> run_dbt
