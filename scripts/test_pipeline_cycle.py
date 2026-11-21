import json
import time
from datetime import datetime, timezone
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from kafka import KafkaProducer, KafkaConsumer
import sys
import os

# Set environment
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"

PROJECT_ID = "ferrous-coda-515405-q2"
LOCATION = "US"
TOPIC = "weather_raw"
BOOTSTRAP_SERVERS = "localhost:9092"

CITIES = {
    "Kolkata":   (22.5726, 88.3639),
    "Delhi":     (28.6139, 77.2090),
    "Mumbai":    (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai":   (13.0827, 80.2707),
}

print("=" * 60)
print("1. PRODUCER: Fetching live weather from Open-Meteo API...")
print("=" * 60)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=10000
)

sent_count = 0
for city, (lat, lon) in CITIES.items():
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code",
            "timezone": "auto",
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        cur = resp.json()["current"]
        record = {
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "observed_at": cur["time"],
            "temperature_c": float(cur["temperature_2m"]),
            "humidity_pct": float(cur["relative_humidity_2m"]),
            "wind_speed_kmh": float(cur["wind_speed_10m"]),
            "precipitation_mm": float(cur["precipitation"]),
            "weather_code": int(cur["weather_code"]),
        }
        producer.send(TOPIC, key=city, value=record)
        print(f" -> Sent {city}: {record['temperature_c']}°C, {record['humidity_pct']}% humidity")
        sent_count += 1
    except Exception as e:
        print(f"Error fetching {city}: {e}")

producer.flush()
print(f"Total sent to Kafka: {sent_count} events.\n")

print("=" * 60)
print("2. CONSUMER: Reading from Kafka and Loading into BigQuery...")
print("=" * 60)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-weather-loader',
    consumer_timeout_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

rows = []
for msg in consumer:
    r = msg.value
    observed = datetime.fromisoformat(r["observed_at"]).strftime("%Y-%m-%d %H:%M:%S")
    rows.append({
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
    })

consumer.close()
print(f"Consumed {len(rows)} messages from Kafka.")

if rows:
    creds = service_account.Credentials.from_service_account_file("service-account-key.json")
    client = bigquery.Client(credentials=creds, project=PROJECT_ID, location=LOCATION)
    table_id = f"{PROJECT_ID}.weather_raw.raw_weather"

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

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    print(f"Triggering BigQuery batch load job for {len(rows)} rows...")
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    print("Batch load completed successfully!")

    print("\n" + "=" * 60)
    print("3. VERIFY: Querying raw_weather from BigQuery...")
    print("=" * 60)
    query = f"SELECT city, observed_at, temperature_c, humidity_pct, ingested_at FROM `{table_id}` ORDER BY ingested_at DESC LIMIT 10"
    df = client.query(query).to_dataframe()
    print(df.to_string(index=False))
