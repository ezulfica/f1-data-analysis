from google.cloud import bigquery
from dotenv import dotenv_values
from utils.loading import load_and_transform_data, load_into_bigquery, delete_local_files
import json 

# === CONFIGURATION ===
CONFIG_PATH = "config/.env"
SETTINGS_PATH = "config/settings.yaml"
config = dotenv_values(CONFIG_PATH)

BQ_PROJECT = config["BQ_PROJECT"]
BQ_DATASET = config["BQ_DATASET"]
SERVICE_ACCOUNT = config["SERVICE_ACCOUNT_JSON_PATH"]

# === Initialize BigQuery Client ===
bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT)

# === Load Data Schema ===
with open("config/data_schema.json", "r") as schema_file:
    schema_json = json.load(schema_file)

# === Main function ===
def main():
    transformed_data = load_and_transform_data(folder="prep", schema_json=schema_json)
    load_into_bigquery(transformed_data, BQ_PROJECT, BQ_DATASET, schema_json, bq_client)
    delete_local_files(folder = "prep")

# === RUN PIPELINE ===
if __name__ == "__main__":
    main()