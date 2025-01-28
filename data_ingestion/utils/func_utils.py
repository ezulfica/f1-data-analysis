import logging
from datetime import date, timedelta
import json
from time import sleep
from data_ingestion.src.f1_api import F1API
from utils.s3_connect import S3Client
from utils.s3_utils import read_object_into_json


def get_last_date(f1_schedule : list) : 
    date = max([schedule['date'] for schedule in f1_schedule])
    return date

def fetch_f1_schedule(s3_client:S3Client, config:dict, f1_api:F1API):
    """Retrieve the F1 schedule from S3 or update it if outdated."""
    s3_f1_schedule_file = f'{f1_api.folder_name}/f1_schedule.json'
    
    f1_schedule = read_object_into_json(
        s3_client=s3_client,
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

def process_race_data(f1_api : F1API, config : dict, LOOKBACK_DAYS : int, get_all: bool):
    """Process and fetch race data."""
    
    if get_all == False : 
        today = date.today()
        date_range_start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        date_range_end = today.strftime("%Y-%m-%d")

        f1_api.pending_races = [
            race for race in f1_api.f1_schedule
            if date_range_start <= race['date'] <= date_range_end
        ]
    else :  
        f1_api.pending_races = f1_api.f1_schedule

    # logging.info("Fetching race results...")
    # f1_api.fetch_seasons_results()
    # sleep(2)
    logging.info("Fetching race data...")
    f1_api.fetch_races_data()
    
 
