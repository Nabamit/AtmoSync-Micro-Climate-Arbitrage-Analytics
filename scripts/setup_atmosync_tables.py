import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ferrous-coda-515405-q2")
LOCATION = os.getenv("BQ_LOCATION", "US")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account-key.json")

creds = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=creds, project=PROJECT_ID, location=LOCATION)

with open("sql/02_atmosync_tables.sql", "r") as f:
    sql = f.read()

statements = [s.strip() for s in sql.split(";") if s.strip()]
for stmt in statements:
    print(f"Executing DDL statement...")
    query_job = client.query(stmt)
    query_job.result()
    print(" -> Done.")

print("All 4 AtmoSync raw tables created successfully in BigQuery!")
