import json
import time
from datetime import datetime, timezone
import os

from google.cloud import bigquery
from google.oauth2 import service_account

import config

BATCH_SIZE = 500
FLUSH_SECONDS = 120     # keeps load jobs well under BigQuery's 1,500 per table per day

F = bigquery.SchemaField
TABLES = {
    config.TOPIC_TELEMETRY: ("raw_container_telemetry", [
        F("container_id", "STRING", mode="REQUIRED"), F("vessel_mmsi", "STRING"), F("commodity", "STRING"),
        F("observed_at", "TIMESTAMP", mode="REQUIRED"), F("internal_temp_c", "FLOAT"),
        F("internal_humidity_pct", "FLOAT"), F("setpoint_temp_c", "FLOAT"), F("door_open", "BOOLEAN"),
        F("ingested_at", "TIMESTAMP", mode="REQUIRED")]),
    config.TOPIC_VESSEL: ("raw_vessel_position", [
        F("mmsi", "STRING", mode="REQUIRED"), F("ship_name", "STRING"), F("latitude", "FLOAT"),
        F("longitude", "FLOAT"), F("sog_knots", "FLOAT"), F("cog_deg", "FLOAT"), F("source", "STRING"),
        F("observed_at", "TIMESTAMP", mode="REQUIRED"), F("ingested_at", "TIMESTAMP", mode="REQUIRED")]),
    config.TOPIC_ROUTE_WEATHER: ("raw_route_weather", [
        F("container_id", "STRING", mode="REQUIRED"), F("vessel_mmsi", "STRING"), F("latitude", "FLOAT"),
        F("longitude", "FLOAT"), F("observed_at", "TIMESTAMP", mode="REQUIRED"), F("ambient_temp_c", "FLOAT"),
        F("ambient_humidity_pct", "FLOAT"), F("wind_speed_kmh", "FLOAT"), F("precipitation_mm", "FLOAT"),
        F("wave_height_m", "FLOAT"), F("sea_surface_temp_c", "FLOAT"),
        F("ingested_at", "TIMESTAMP", mode="REQUIRED")]),
    config.TOPIC_MARKET: ("raw_market_price", [
        F("port_id", "STRING", mode="REQUIRED"), F("price_region", "STRING"),
        F("observed_at", "TIMESTAMP", mode="REQUIRED"), F("price_per_unit_usd", "FLOAT"),
        F("source", "STRING"), F("ingested_at", "TIMESTAMP", mode="REQUIRED")]),
}


def get_bigquery_client():
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        creds = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(credentials=creds, project=config.GCP_PROJECT_ID, location=config.BQ_LOCATION)
    return bigquery.Client(project=config.GCP_PROJECT_ID, location=config.BQ_LOCATION)


def flush(client, buffers):
    for topic, rows in buffers.items():
        if not rows:
            continue
        table_name, schema = TABLES[topic]
        table_id = f"{client.project}.{config.BQ_RAW_DATASET}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
        )
        client.load_table_from_json(rows, table_id, job_config=job_config).result()
        print(f"Loaded {len(rows):>5} rows into {table_name}")


def run():
    client = get_bigquery_client()
    topics = list(TABLES.keys())
    print(f"[AtmoSync Consumer] Starting multi-topic consumer for: {topics}")

    try:
        from confluent_kafka import Consumer as ConfluentConsumer
        consumer = ConfluentConsumer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": "atmosync-bigquery-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(topics)
        is_confluent = True
    except ImportError:
        from kafka import KafkaConsumer as PythonKafkaConsumer
        consumer = PythonKafkaConsumer(
            *topics,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            group_id="atmosync-bigquery-writer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        is_confluent = False

    buffers = {t: [] for t in TABLES}
    count, batch_started = 0, None
    print(f"[AtmoSync Consumer] Ready and listening for events...")

    try:
        while True:
            if is_confluent:
                msg = consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                    else:
                        row = json.loads(msg.value().decode("utf-8"))
                        row["ingested_at"] = datetime.now(timezone.utc).isoformat()
                        buffers[msg.topic()].append(row)
                        count += 1
                        if batch_started is None:
                            batch_started = time.time()
            else:
                for msg in consumer:
                    row = msg.value
                    row["ingested_at"] = datetime.now(timezone.utc).isoformat()
                    buffers[msg.topic].append(row)
                    count += 1
                    if batch_started is None:
                        batch_started = time.time()
                    if count >= BATCH_SIZE or (time.time() - batch_started >= FLUSH_SECONDS):
                        break

            if count and (count >= BATCH_SIZE or time.time() - batch_started >= FLUSH_SECONDS):
                try:
                    flush(client, buffers)
                    if is_confluent:
                        consumer.commit(asynchronous=False)
                    else:
                        consumer.commit()
                    buffers = {t: [] for t in TABLES}
                    count, batch_started = 0, None
                except Exception as e:
                    print(f"Load failed, will retry in 10s: {e}")
                    time.sleep(10)
    except KeyboardInterrupt:
        print("[AtmoSync Consumer] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
