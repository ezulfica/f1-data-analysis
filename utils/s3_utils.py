import json
import logging
import polars as pl

from utils.s3_connect import S3Client
from data_ingestion.src.f1_api import F1API


def connect_s3(config) -> S3Client:
    """Establish a connection to AWS S3."""
    s3_client = S3Client(
        aws_access_key_id=config["aws_access_key"],
        aws_secret_access_key=config["aws_secret_key"],
        region_name=config["aws_region"],
        bucket_name=config["s3_bucket"]
    )
    s3_client.connect()
    return s3_client

def read_object_into_json(s3_client: S3Client, object_key : str) -> None: 
    obj = s3_client.read_object(object_key)
    return json.loads(obj)

def read_object_into_table(s3_client : S3Client, object_key : str) -> tuple : 
    data = read_object_into_json(s3_client,object_key)
    dt = pl.DataFrame(data["MRData"]["RaceTable"]["Races"])
    if dt.shape != (0,0) : 
        return dt, object_key

def upload_results(s3_client : S3Client, f1_api : F1API):
    """Upload F1 results to S3."""
    for result in f1_api.f1_seasons_results + f1_api.f1_races_data:
        filename, data = result
        logging.info(f"Uploading {filename} to S3...")
        s3_client.write_object(object_key=filename, data=data)