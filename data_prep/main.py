from utils.s3_utils import connect_s3
from utils.prep import data_loading_concurrency, file_to_prep
from utils.config import get_config

SETTINGS_PATH = "config/settings.yaml"


def main():
    config = get_config()
    s3_client = connect_s3(config)
    s3_files = file_to_prep("raw/")
    s3_files.sort()
    data_loading_concurrency(s3_client=s3_client, files=s3_files)

if __name__ == "__main__":
    main()
