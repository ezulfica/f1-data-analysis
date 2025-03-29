import json
import logging
from concurrent.futures import ThreadPoolExecutor
from utils.s3_client import S3Client
from utils.config import get_all_file_paths
from pathlib import Path


def connect_s3(config: dict) -> S3Client:
    """
    Establish a connection to AWS S3 using the provided configuration.

    Args:
        config (dict): Configuration dictionary containing AWS credentials, region, and bucket name.

    Returns:
        S3Client: The established S3 client object.

    This function creates an S3Client instance using the credentials and connection details 
    from the provided config and calls the `connect()` method to establish the connection.
    """
    s3_client = S3Client(
        aws_access_key_id=config["AWS_ACCESS_KEY"],
        aws_secret_access_key=config["AWS_SECRET_KEY"],
        region_name=config["aws_region"],
        bucket_name=config["s3_bucket"],
    )
    s3_client.connect()
    return s3_client


def read_object_into_json(s3_client: S3Client, object_key: str) -> None:
    """
    Read an object from S3 and load it into a JSON object.

    Args:
        s3_client (S3Client): The connected S3 client instance.
        object_key (str): The key (path) of the object in the S3 bucket.

    Returns:
        dict: The parsed JSON object.

    This function reads an object from S3 using the `read_object()` method of the S3Client,
    and then converts the byte data into a Python dictionary by using `json.loads()`.
    """
    
    obj = s3_client.read_object(object_key)
    return json.loads(obj)


def upload_file(s3_client: S3Client, filename: str) -> None:
    """
    Upload a file to S3 after reading it and converting its contents to JSON.

    Args:
        s3_client (S3Client): The connected S3 client instance.
        filename (str): The local file to be uploaded.

    This function creates the necessary directories for the file path, reads the file,
    converts its contents to JSON, and uploads it to S3 using the `write_object()` method of
    the `S3Client`. 
    """
    folder = "/".join(filename.split("/")[:-1])
    Path(folder).mkdir(parents=True, exist_ok=True)

    with open(filename, "r") as file:
        data = json.dumps(json.load(file))

    # logging.info(f"Uploading {filename} to S3...")
    s3_client.write_object(object_key=filename, data=data)


def upload_results(s3_client: S3Client, foldername: str) -> None:
    """
    Upload all files from a specified folder to S3 using parallel processing.

    Args:
        s3_client (S3Client): The connected S3 client instance.
        foldername (str): The local folder containing the files to be uploaded.

    This function retrieves all file paths in the given folder, then uses a `ThreadPoolExecutor`
    to upload the files concurrently (up to 3 files at a time). Once all files are uploaded,
    a success message is logged.
    """

    filelist = get_all_file_paths(foldername)

    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            uploaded_file = [
                executor.submit(upload_file(s3_client, filename))
                for filename in filelist
            ]
        logging.info("Files uploaded in S3!")
    except:
        logging.info("no list to upload")
