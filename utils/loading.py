from google.cloud import bigquery
from pathlib import Path
import polars as pl
import pandas as pd
from pathlib import Path


def list_file(folder: str) -> list:
    path_ = list(Path(folder).rglob("*"))
    parquet_files = [filename for filename in path_ if filename.suffix == ".parquet"]
    return parquet_files


def file_to_load(folder: str) -> dict:
    # Get all files in directory recursively
    parquet_files = list_file(folder)
    foldernames = set([filename.parent.name for filename in parquet_files])

    files_list = {
        foldername: [
            str(filename)
            for filename in parquet_files
            if filename.parent.name == foldername
        ]
        for foldername in foldernames
    }
    return files_list


def read_data_and_concat(files_to_concat: list) -> pl.DataFrame:
    # Read all files in the list and concatenate them
    dfs = [pl.read_parquet(file) for file in files_to_concat]
    dfs = pl.concat(dfs, how="diagonal")
    return dfs.to_pandas()


def load_and_concat(folder: str) -> dict:
    # Read all files in the folder and concatenate them
    files = file_to_load(folder)
    return {table: read_data_and_concat(files[table]) for table in files.keys()}


def convert_pandas_df(df: pd.DataFrame, schema_json: dict) -> pd.DataFrame:
    # Apply type conversions
    for col, dtype in schema_json.items():
        try:
            if dtype == "INTEGER":
                # Convert non-numeric values to NaN and then to Int64
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "FLOAT":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif dtype == "DATE":
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
            elif dtype == "STRING":
                df[col] = df[col].astype("string")
        except Exception as e:
            print(f"Error converting column {col} to {dtype}: {e}")

    return df


# === Load and Transform Data ===
def load_and_transform_data(folder: str, schema_json: dict) -> dict:
    datasets = load_and_concat(folder)  # Load data


    if "schedule" in datasets.keys() : 
        datasets["schedule"] = datasets.pop(folder)

    transformed_datasets = {}
    for table, df in datasets.items():
        print(f"Processing table: {table}")
        try:
            converted_df = convert_pandas_df(df, schema_json[table])
            transformed_datasets[table] = converted_df
        except Exception as e:
            print(f"Error transforming table {table}: {e}")

    return transformed_datasets


# === Load Data into BigQuery ===
def load_into_bigquery(
    data: pd.DataFrame,
    project: str,
    dataset: str,
    schema_json: dict,
    bq_client: bigquery.Client,
) -> None:
    for table, df in data.items():
        table_id = f"{project}.{dataset}.{table}"
        schema = [
            bigquery.SchemaField(name, data_type)
            for name, data_type in schema_json[table].items()
        ]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )

        print(f"Loading {table} into BigQuery...")
        try:
            load_job = bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            load_job.result()  # Wait for completion
            print(f"Successfully loaded {table} into BigQuery ✅")
        except Exception as e:
            print(f"Error loading {table} into BigQuery ❌: {e}")


def delete_local_files(folder: str):
    return [
        file.unlink() for file in list_file(folder)
    ]  # delete the files after loading
