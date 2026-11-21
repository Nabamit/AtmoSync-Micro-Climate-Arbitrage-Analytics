import json
import time
from datetime import datetime, timezone
import os

from google.cloud import bigquery
from google.oauth2 import service_account
import config

BATCH_SIZE = 50        # Flush when this many rows are accumulated
FLUSH_SECONDS = 20     # Or when this many seconds have passed since first row

SCHEMA = [
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


def get_bigquery_client() -> bigquery.Client:
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        creds = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(credentials=creds, project=config.GCP_PROJECT_ID, location=config.BQ_LOCATION)
    return bigquery.Client(project=config.GCP_PROJECT_ID, location=config.BQ_LOCATION)


def to_row(r: dict) -> dict:
    observed = datetime.fromisoformat(r["observed_at"]).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "city": r["city"],
        "latitude": r.get("latitude"),
        "longitude": r.get("longitude"),
        "observed_at": observed,
        "temperature_c": r.get("temperature_c"),
        "humidity_pct": r.get("humidity_pct"),
        "wind_speed_kmh": r.get("wind_speed_kmh"),
        "precipitation_mm": r.get("precipitation_mm"),
        "weather_code": r.get("weather_code"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def load_batch(client: bigquery.Client, table_id: str, rows: list):
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # Wait for batch load completion (Batch load jobs are free in BigQuery Sandbox)


def run():
    client = get_bigquery_client()
    table_id = f"{client.project}.{config.BQ_RAW_DATASET}.{config.BQ_RAW_TABLE}"
    print(f"[Consumer] Writing to BigQuery Table: {table_id} (Location: {config.BQ_LOCATION})")

    try:
        from confluent_kafka import Consumer as ConfluentConsumer
        consumer = ConfluentConsumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": "weather-bigquery-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([config.TOPIC])
        is_confluent = True
    except ImportError:
        from kafka import KafkaConsumer as PythonKafkaConsumer
        consumer = PythonKafkaConsumer(
            config.TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id="weather-bigquery-writer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        is_confluent = False

    rows, batch_started = [], None
    print(f"[Consumer] Listening to Kafka topic '{config.TOPIC}'...")

    try:
        while True:
            msg_val = None
            if is_confluent:
                msg = consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        print(f"[Consumer] Kafka error: {msg.error()}")
                    else:
                        msg_val = json.loads(msg.value().decode("utf-8"))
            else:
                for msg in consumer:
                    rows.append(to_row(msg.value))
                    if batch_started is None:
                        batch_started = time.time()
                    if len(rows) >= BATCH_SIZE or (time.time() - batch_started >= FLUSH_SECONDS):
                        break

            if is_confluent and msg_val is not None:
                rows.append(to_row(msg_val))
                if batch_started is None:
                    batch_started = time.time()

            if rows and (len(rows) >= BATCH_SIZE or (time.time() - batch_started >= FLUSH_SECONDS)):
                try:
                    load_batch(client, table_id, rows)
                    if is_confluent:
                        consumer.commit(asynchronous=False)
                    else:
                        consumer.commit()
                    print(f"[Consumer] Successfully loaded {len(rows)} rows into {table_id}")
                    rows, batch_started = [], None
                except Exception as e:
                    print(f"[Consumer] Load failed, retrying in 10s: {e}")
                    time.sleep(10)
    except KeyboardInterrupt:
        print("[Consumer] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
