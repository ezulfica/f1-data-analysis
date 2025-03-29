from google.cloud import bigquery
from pathlib import Path
import polars as pl
import pandas as pd
from pathlib import Path


def list_file(folder: str) -> list:
    """
    Lists all Parquet files in a given folder recursively.

    Args:
        folder (str): Path to the folder.

    Returns:
        list: A list of Path objects representing the Parquet files.
    """
    path_ = list(Path(folder).rglob("*"))
    parquet_files = [filename for filename in path_ if filename.suffix == ".parquet"]
    return parquet_files


def file_to_load(folder: str) -> dict:
    """
    Organizes Parquet files into a dictionary by their parent folder name.

    Args:
        folder (str): Path to the folder.

    Returns:
        dict: A dictionary where keys are folder names and values are lists of file paths.
    """
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
    """
    Reads multiple Parquet files and concatenates them into a single Polars DataFrame.

    Args:
        files_to_concat (list): List of Parquet file paths.

    Returns:
        pd.DataFrame: Concatenated Pandas DataFrame.
    """
    dfs = [pl.read_parquet(file) for file in files_to_concat]
    dfs = pl.concat(dfs, how="diagonal")
    return dfs.to_pandas()


def load_and_concat(folder: str) -> dict:
    """
    Loads and concatenates all Parquet files from a given folder, grouping them by table.

    Args:
        folder (str): Path to the folder containing the Parquet files.

    Returns:
        dict: A dictionary where keys are table names and values are concatenated DataFrames.
    """
    files = file_to_load(folder)
    return {table: read_data_and_concat(files[table]) for table in files.keys()}


def convert_pandas_df(df: pd.DataFrame, schema_json: dict) -> pd.DataFrame:
    """
    Converts a Pandas DataFrame's columns to the specified schema.

    Args:
        df (pd.DataFrame): The DataFrame to be converted.
        schema_json (dict): A dictionary mapping column names to target data types.

    Returns:
        pd.DataFrame: The DataFrame with the specified type conversions.
    """
    for col, dtype in schema_json.items():

        if col not in df.columns:
            print(f"Warning: Column '{col}' is missing. Adding it with NULL values.")
            df[col] = pd.NA  # Add column with NULL values

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
    """
    Loads, concatenates, and transforms data according to the provided schema.

    Args:
        folder (str): Path to the folder containing data.
        schema_json (dict): Dictionary specifying column types for transformation.

    Returns:
        dict: A dictionary of transformed DataFrames.
    """
    
    datasets = load_and_concat(folder)  # Load data


    # if "schedule" in datasets.keys() : 
    #     datasets["schedule"] = datasets.pop(folder)

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
    
    """
    Loads transformed data into BigQuery.

    Args:
        data (dict): A dictionary where keys are table names and values are Pandas DataFrames.
        project (str): Google Cloud project ID.
        dataset (str): BigQuery dataset name.
        schema_json (dict): Schema definition for each table.
        bq_client (bigquery.Client): BigQuery client instance.

    Behavior:
        - Converts the schema dictionary into BigQuery schema fields.
        - Loads the data into BigQuery with "WRITE_TRUNCATE" mode.
        - Handles any errors during the upload process.
    """

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


def delete_local_files(folder: str) -> None:
    """
    Deletes all Parquet files in the specified folder after processing.

    Args:
        folder (str): Path to the folder containing the files.

    Returns:
        list: A list of deleted files.
    """

    return [
        file.unlink() for file in list_file(folder)
    ]  # delete the files after loading
