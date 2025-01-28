import polars as pl
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.s3_connect import S3Client
from utils.s3_utils import read_object_into_table 

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
        dt, filename = read_object_into_table(s3_client, object_key)
        filename = filename.replace("raw", "prep").replace("json", "parquet")
        dt = recursive_unnest_explode(df = dt, parent_prefix= "")
        load_data_to_s3_as_parquet(df = dt, s3_client = s3_client, filename=filename)
    except : 
        None

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

def load_data_to_s3_as_parquet(df: pl.DataFrame, s3_client : S3Client, filename : str) -> None:
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
        s3_client.write_object(object_key= filename, data=buffer.getvalue())
        print(f"Successfully uploaded {filename} to {s3_client.bucket_name}.")
    except Exception as e:
        print(f"Error uploading Polars DataFrame to S3: {e}")