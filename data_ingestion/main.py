import json
from datetime import date, timedelta
from dotenv import dotenv_values
import logging

from data_ingestion.src.f1_api import F1API
from data_ingestion.src.utils import read_object_into_json, get_last_date
from aws_connect.s3_connect import S3Client


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
CONFIG_PATH = "config/.env"
LOOKBACK_DAYS = 5

def load_config():
    """Load environment variables from config file."""
    config = dotenv_values(CONFIG_PATH)
    return {
        "aws_access_key": config["AWS_ACCESS_KEY"],
        "aws_secret_key": config["AWS_SECRET_KEY"],
        "s3_bucket": config["s3_bucket"],
        "s3_prefix_folder": config["s3_prefix_folder"],
        "aws_region": config["aws_region"],
    }

def connect_s3(config):
    """Establish a connection to AWS S3."""
    s3_client = S3Client(
        aws_access_key_id=config["aws_access_key"],
        aws_secret_access_key=config["aws_secret_key"],
        region_name=config["aws_region"]
    )
    s3_client.connect()
    return s3_client

def fetch_f1_schedule(s3_client, config):
    """Retrieve the F1 schedule from S3 or update it if outdated."""
    s3_f1_schedule_file = f'{config["s3_prefix_folder"]}/f1_schedule.json'
    
    f1_schedule = read_object_into_json(
        s3_client, 
        bucket_name=config["s3_bucket"], 
        object_key=s3_f1_schedule_file
        )

    last_schedule_date = get_last_date(f1_schedule)
    today = date.today().strftime("%Y-%m-%d")

    if last_schedule_date <= today:
        logging.info("Updating F1 schedule...")
        f1_api.fetch_f1_seasons_schedule()
        s3_client.write_object(
            bucket_name=config["s3_bucket"],
            object_key=s3_f1_schedule_file,
            data=json.dumps(f1_api.f1_schedule),
        )
    else:
        logging.info("Using existing F1 schedule.")
        f1_api.f1_schedule = f1_schedule

def process_race_data(f1_api, config):
    """Process and fetch race data."""
    today = date.today()
    date_range_start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    date_range_end = today.strftime("%Y-%m-%d")

    f1_api.pending_races = [
        race for race in f1_api.f1_schedule
        if date_range_start <= race['date'] <= date_range_end
    ]

    f1_api.folder_name = config["s3_prefix_folder"]

    logging.info("Fetching race data...")
    f1_api.fetch_races_data()
    f1_api.fetch_seasons_results()

def upload_results(s3_client, f1_api, bucket_name):
    """Upload F1 results to S3."""
    for result in f1_api.f1_seasons_results + f1_api.f1_races_data:
        filename, data = result
        logging.info(f"Uploading {filename} to S3...")
        s3_client.write_object(bucket_name=bucket_name, object_key=filename, data=data)

def main():
    """Main execution function."""
    config = load_config()
    s3_client = connect_s3(config)

    f1_api = F1API()
    fetch_f1_schedule(s3_client, config)
    process_race_data(f1_api, config)
    upload_results(s3_client, f1_api, config["s3_bucket"])

if __name__ == "__main__":
    main()