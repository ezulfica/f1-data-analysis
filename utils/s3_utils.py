import json
import logging
from concurrent.futures import ThreadPoolExecutor
from utils.s3_client import S3Client
from utils.config import get_all_file_paths
from pathlib import Path

def connect_s3(config) -> S3Client:
    """Establish a connection to AWS S3."""
    s3_client = S3Client(
        aws_access_key_id=config["AWS_ACCESS_KEY"],
        aws_secret_access_key=config["AWS_SECRET_KEY"],
        region_name=config["aws_region"],
        bucket_name=config["s3_bucket"]
    )
    s3_client.connect()
    return s3_client

def read_object_into_json(s3_client: S3Client, object_key : str) -> None: 
    obj = s3_client.read_object(object_key)
    return json.loads(obj)


def upload_file(s3_client : S3Client, filename : str):
    """Upload F1 results to S3."""

    folder = "/".join(filename.split("/")[:-1])
    Path(folder).mkdir(parents=True, exist_ok=True)
    
    with open(filename, "r") as file : 
        data = json.dumps(json.load(file))

    #logging.info(f"Uploading {filename} to S3...")
    s3_client.write_object(object_key=filename, data=data)
        
def upload_results(s3_client: S3Client, foldername: str) -> None : 
    filelist = get_all_file_paths(foldername)

    txt = "\n".join(filelist)
    s3_client.write_object(object_key=uploaded_file, data=txt)

    with ThreadPoolExecutor(max_workers=3) as executor : 
        uploaded_file = [
            executor.submit(upload_file(s3_client, filename)) for filename in filelist
            ]
    logging.info("Files uploaded in S3!")
