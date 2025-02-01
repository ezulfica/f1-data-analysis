#!/home/eric/Data/F1 Data Analysis/.venv/bin/python

import logging
import os
from data_ingestion.src.f1_api import F1API  # Absolute import
from utils.config_utils import load_config
from utils.s3_utils import connect_s3, upload_results
from data_ingestion.utils.func_utils import fetch_f1_schedule, process_race_data
import polars as pl

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
CONFIG_PATH = "config/.env"
LOOKBACK_DAYS = 5

#main function
def main():
    """Main execution function."""
    config = load_config(CONFIG_PATH)
    s3_client = connect_s3(config)
    f1_api = F1API()
    fetch_f1_schedule(s3_client, config, f1_api)
    process_race_data(f1_api, config, LOOKBACK_DAYS, get_all=False)
    upload_results(s3_client, f1_api.folder_name)

if __name__ == "__main__":
    main()
