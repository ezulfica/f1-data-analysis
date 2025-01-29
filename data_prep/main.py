from utils.s3_utils import connect_s3
from utils.config_utils import load_config
from data_prep.utils.utils import data_loading_concurrency
from data_prep.utils.utils import file_to_prep

SETTINGS_PATH = "config/settings.yaml"
CONFIG_PATH = "config/.env"


def main() : 
    config = load_config(CONFIG_PATH)
    s3_client = connect_s3(config)
    s3_files = file_to_prep("raw/")
    data_loading_concurrency(s3_client=s3_client, files = s3_files)

if __name__ == "__main__":
    main()

