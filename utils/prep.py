import polars as pl
import json
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from utils.s3_client import S3Client


def file_to_prep(directory: str) -> list:
    """
    Scans a directory recursively and returns a list of all JSON files.

    Args:
        directory (str): Path to the directory.

    Returns:
        list: List of file paths with a .json extension.
    """
    path_ = list(Path(directory).rglob("*"))
    return [str(filename) for filename in path_ if filename.suffix == ".json"]


def data_loading_concurrency(s3_client: S3Client, files: list) -> None:
    """
    Processes multiple JSON files concurrently and uploads them to S3 after transformation.

    Args:
        s3_client (S3Client): An instance of the S3 client.
        files (list): List of file paths to process.

    Returns:
        str: Completion message.
    """
    num_files = len(files)

    with tqdm(total=num_files) as pbar:
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = [
                executor.submit(prep_data_into_s3, s3_client, object_key)
                for object_key in files
            ]
            for _ in as_completed(future_to_file):
                pbar.update(1)
    return "Completed !"


def prep_data_into_s3(s3_client: S3Client, object_key: str) -> None:
    """
    Reads, transforms, and uploads a JSON file to S3 in Parquet format.

    Args:
        s3_client (S3Client): S3 client for uploading files.
        object_key (str): File path of the JSON object.
    """
    try:
        dt, filename = read_object_into_table(object_key)
        new_filename = filename.replace("raw", "prep").replace("json", "parquet")
        dt = recursive_unnest_explode(df=dt, parent_prefix="")
        load_data_to_s3_as_parquet(
            df=dt, s3_client=s3_client, filename=new_filename, old_filename=filename
        )
    except:
        None


def read_object_into_table(object_key: str) -> tuple:
    """
    Reads a JSON file into a Polars DataFrame, processing F1 race data.

    Args:
        object_key (str): Path to the JSON file.

    Returns:
        tuple: A Polars DataFrame and the object key (filename).
    """
    with open(object_key, "r") as file:
        data = json.load(file)
    # dt = pl.concat([pl.DataFrame(dt["MRData"]["RaceTable"]["Races"]) for dt in data])
    if "f1_schedule" in object_key:
        dt = pl.DataFrame(data)
    else:
        dataframes = [
            pl.DataFrame(dt["MRData"]["RaceTable"]["Races"] if "RaceTable" in dt["MRData"].keys() 
                         else dt["MRData"]["StandingsTable"]["StandingsLists"]) for dt in data
                         ]
        

        dt = pl.concat(dataframes, how = "diagonal_relaxed")
    if dt.shape != (0, 0):
        return dt, object_key


def recursive_unnest_explode(df: pl.DataFrame, parent_prefix: str = "") -> pl.DataFrame:
    """
    Recursively unnests (flattens) struct columns and explodes list columns in a Polars DataFrame.

    Args:
        df (pl.DataFrame): Input Polars DataFrame.
        parent_prefix (str, optional): Prefix for nested column names. Defaults to "".

    Returns:
        pl.DataFrame: Transformed DataFrame with nested structures flattened.
    """
    # Identify struct (nested dict) and list (array) columns
    struct_cols = [col for col in df.columns if isinstance(df[col].dtype, pl.Struct)]
    list_cols = [col for col in df.columns if isinstance(df[col].dtype, pl.List)]

    # Handle struct columns by prefixing their fields
    for col in struct_cols:
        new_columns = {
            field: f"{parent_prefix}{col}_{field}" for field in df[col].struct.fields
        }
        df = df.with_columns(
            pl.col(col).struct.rename_fields(list(new_columns.values()))
        )
        df = df.unnest(col)

    # Handle list columns by exploding them
    if list_cols:
        df = df.explode(list_cols)

    # Check recursively for further nested structs or lists
    if any(isinstance(df[col].dtype, (pl.Struct, pl.List)) for col in df.columns):
        df = recursive_unnest_explode(df, parent_prefix=parent_prefix)

    return df


def load_data_to_s3_as_parquet(
    df: pl.DataFrame, s3_client: S3Client, filename: str, old_filename: str
) -> None:
    """
    Writes a Polars DataFrame to S3 as a Parquet file with Gzip compression.

    Args:
        df (pl.DataFrame): The DataFrame to upload.
        s3_client (S3Client): S3 client for handling file upload.
        filename (str): New filename (Parquet format).
        old_filename (str): Original JSON filename (to be deleted post-upload).

    Behavior:
        - Saves a local copy of the Parquet file before uploading.
        - Uses Gzip compression for efficient storage.
        - Deletes the original JSON file after a successful upload.
    """
    try:
        buffer = BytesIO()
        df.write_parquet(file=buffer, compression="gzip")
        folder = "/".join(filename.split("/")[:-1])
        Path(folder).mkdir(parents=True, exist_ok=True)
        df.write_parquet(
            filename, compression="gzip"
        )  # get a copy in local for the loading part in the database
        s3_client.write_object(object_key= filename, data=buffer.getvalue())
        #print(f"Successfully uploaded {filename} to {s3_client.bucket_name}.")
        Path(old_filename).unlink()
    except Exception as e:
        print(f"Error uploading Polars DataFrame to S3: {e}")
