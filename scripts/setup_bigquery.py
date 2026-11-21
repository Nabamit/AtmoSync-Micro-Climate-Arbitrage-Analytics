import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
LOCATION = os.getenv("BQ_LOCATION", "US")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

print(f"Connecting to BigQuery for project: {PROJECT_ID} in location: {LOCATION}...")

if KEY_PATH and os.path.exists(KEY_PATH):
    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=creds, project=PROJECT_ID, location=LOCATION)
else:
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

# 1. Create Datasets
datasets = ["weather_raw", "weather_analytics"]
for ds_name in datasets:
    ds_id = f"{PROJECT_ID}.{ds_name}"
    dataset = bigquery.Dataset(ds_id)
    dataset.location = LOCATION
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset created / verified: {dataset.dataset_id}")

# 2. Create raw_weather Table
raw_table_id = f"{PROJECT_ID}.weather_raw.raw_weather"
schema = [
    bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("observed_at", "DATETIME", mode="REQUIRED"),
    bigquery.SchemaField("temperature_c", "FLOAT"),
    bigquery.SchemaField("humidity_pct", "FLOAT"),
    bigquery.SchemaField("wind_speed_kmh", "FLOAT"),
    bigquery.SchemaField("precipitation_mm", "FLOAT"),
    bigquery.SchemaField("weather_code", "INTEGER"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

table = bigquery.Table(raw_table_id, schema=schema)
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="ingested_at",
)
table.clustering_fields = ["city"]

table = client.create_table(table, exists_ok=True)
print(f"Table created / verified: {table.table_id} (Partitioned by ingested_at, Clustered by city)")
print("BigQuery setup complete!")
