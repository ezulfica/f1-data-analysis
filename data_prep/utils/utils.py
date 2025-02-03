import polars as pl
import json
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from utils.s3_connect import S3Client

def file_to_prep(directory: str) : 
    path_ =  list(Path(directory).rglob("*"))
    return [str(filename) for filename in path_ if filename.suffix == '.json']

def data_loading_concurrency(s3_client: S3Client, files : list) :

    num_files = len(files)

    with tqdm(total = num_files) as pbar : 
        with ThreadPoolExecutor(max_workers=5) as executor : 
            future_to_file = [
                executor.submit(prep_data_into_s3(s3_client, object_key)) 
                for object_key in files
                ]
            for _ in as_completed(future_to_file) :
                pbar.update(1)
    return "Completed !" 


def prep_data_into_s3(s3_client : S3Client, object_key : str) -> None : 
    try : 
        dt, filename = read_object_into_table(object_key)
        new_filename = filename.replace("raw", "prep").replace("json", "parquet")
        dt = recursive_unnest_explode(df = dt, parent_prefix= "")
        load_data_to_s3_as_parquet(df = dt, s3_client = s3_client, filename=new_filename, old_filename = filename)
    except : 
        None

def read_object_into_table(object_key : str) -> tuple : 
    with open(object_key, "r") as file :
        data = json.load(file)
    #dt = pl.concat([pl.DataFrame(dt["MRData"]["RaceTable"]["Races"]) for dt in data])
    dataframes = [pl.DataFrame(dt["MRData"]["RaceTable"]["Races"]) for dt in data]
    aligned_dataframes = align_nested_structs(dataframes)
    dt = pl.concat(aligned_dataframes)
    if dt.shape != (0,0) : 
        return dt, object_key


def align_nested_structs(dataframes):
    """
    Ensures all Struct and List(Struct) columns have the same schema before concatenation.
    """
    # Collect all unique column names
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)

    # Ensure all DataFrames have the same columns (adding missing ones as null)
    dataframes = [
        df.with_columns([pl.lit(None).alias(col) for col in all_columns if col not in df.columns])
        for df in dataframes
    ]

    # Identify Struct and List(Struct) columns
    struct_columns = {}
    list_struct_columns = {}

    for df in dataframes:
        for col, dtype in zip(df.columns, df.dtypes):
            if isinstance(dtype, pl.Struct):
                struct_columns[col] = struct_columns.get(col, set()) | set(dtype.fields)
            elif isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.Struct):
                list_struct_columns[col] = list_struct_columns.get(col, set()) | set(dtype.inner.fields)

    # Align Struct columns
    for col, fields in struct_columns.items():
        for i, df in enumerate(dataframes):
            if col in df.columns:
                current_fields = df.schema[col].fields
                missing_fields = fields - set(current_fields)
                if missing_fields:
                    dataframes[i] = df.with_columns(
                        df[col].struct.rename_fields({field: field for field in current_fields})
                        .struct.update({field: pl.lit(None).cast(pl.Utf8) for field in missing_fields})
                    )

    # Align List(Struct) columns
    for col, fields in list_struct_columns.items():
        for i, df in enumerate(dataframes):
            if col in df.columns:
                current_fields = df.schema[col].inner.fields
                missing_fields = fields - set(current_fields)
                if missing_fields:
                    dataframes[i] = df.with_columns(
                        df[col].list.eval(
                            pl.struct({f: pl.lit(None).cast(pl.Utf8) for f in missing_fields})
                        )
                    )

    return dataframes


def recursive_unnest_explode(df: pl.DataFrame, parent_prefix="") -> pl.DataFrame:
    
    # Identify struct (nested dict) and list (array) columns
    struct_cols = [col for col in df.columns if isinstance(df[col].dtype, pl.Struct)]
    list_cols = [col for col in df.columns if isinstance(df[col].dtype, pl.List)]

    # Handle struct columns by prefixing their fields
    for col in struct_cols:
        new_columns = {field: f"{parent_prefix}{col}_{field}" for field in df[col].struct.fields}
        df = df.with_columns(pl.col(col).struct.rename_fields(list(new_columns.values())))
        df = df.unnest(col)

    # Handle list columns by exploding them
    if list_cols:
        df = df.explode(list_cols)

    # Check recursively for further nested structs or lists
    if any(isinstance(df[col].dtype, (pl.Struct, pl.List)) for col in df.columns):
        df = recursive_unnest_explode(df, parent_prefix=parent_prefix)

    return df

def load_data_to_s3_as_parquet(df: pl.DataFrame, s3_client : S3Client, filename : str, old_filename: str) -> None:
    """
    Write a Polars DataFrame to S3 as a Parquet file with specified compression.

    :param bucket_name: Name of the S3 bucket
    :param object_key: Key (path) of the object in S3
    :param dataframe: Polars DataFrame to be written
    :param compression: Parquet compression type (e.g., 'snappy', 'gzip', 'brotli', 'none')
    """
    try :  
        buffer = BytesIO()
        df.write_parquet(file = buffer, compression="gzip")
        folder = "/".join(filename.split("/")[:-1])
        Path(folder).mkdir(parents=True, exist_ok=True)
        df.write_parquet(filename, compression="gzip") #get a copy in local for the loading part in the database
        s3_client.write_object(object_key= filename, data=buffer.getvalue())
        print(f"Successfully uploaded {filename} to {s3_client.bucket_name}.")
        Path(old_filename).unlink()
    except Exception as e:
        print(f"Error uploading Polars DataFrame to S3: {e}")