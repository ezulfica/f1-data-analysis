from utils.s3_utils import connect_s3
from dotenv import dotenv_values
from utils.prep import data_loading_concurrency, file_to_prep

SETTINGS_PATH = "config/settings.yaml"
CONFIG_PATH = "config/.env"


def main() : 
    config = dotenv_values(CONFIG_PATH)
    s3_client = connect_s3(config)
    s3_files = file_to_prep("raw/")
    data_loading_concurrency(s3_client=s3_client, files = s3_files)

if __name__ == "__main__":
    main()

