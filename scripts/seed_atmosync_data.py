import json
import math
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ferrous-coda-515405-q2")
LOCATION = os.getenv("BQ_LOCATION", "US")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account-key.json")

creds = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=creds, project=PROJECT_ID, location=LOCATION)

print("=" * 60)
print("Seeding initial AtmoSync data directly to BigQuery...")
print("=" * 60)

now = datetime.now(timezone.utc)
ingested_at = now.isoformat()

# 1. Vessels
vessel_rows = [
    {
        "mmsi": "111111111",
        "ship_name": "PACIFIC-RUNNER-1",
        "latitude": 28.50,
        "longitude": -116.20,
        "sog_knots": 18.2,
        "cog_deg": 320.0,
        "source": "sim",
        "observed_at": (now - timedelta(minutes=5)).isoformat(),
        "ingested_at": ingested_at
    },
    {
        "mmsi": "222222222",
        "ship_name": "PACIFIC-CARRIER-2",
        "latitude": 30.10,
        "longitude": -117.80,
        "sog_knots": 17.8,
        "cog_deg": 325.0,
        "source": "sim",
        "observed_at": (now - timedelta(minutes=5)).isoformat(),
        "ingested_at": ingested_at
    }
]

# Write latest vessel state for weather_producer
state_file = Path("state/vessel_latest.json")
state_file.parent.mkdir(exist_ok=True)
state_file.write_text(json.dumps({
    "111111111": {"lat": 28.50, "lon": -116.20, "time": now.isoformat()},
    "222222222": {"lat": 30.10, "lon": -117.80, "time": now.isoformat()}
}))

# 2. Market Prices (Seattle, LosAngeles, SanFrancisco)
market_rows = [
    {"port_id": "SEA", "price_region": "Seattle", "observed_at": now.isoformat(), "price_per_unit_usd": 1.48, "source": "csv_replay", "ingested_at": ingested_at},
    {"port_id": "LAX", "price_region": "LosAngeles", "observed_at": now.isoformat(), "price_per_unit_usd": 1.34, "source": "csv_replay", "ingested_at": ingested_at},
    {"port_id": "OAK", "price_region": "SanFrancisco", "observed_at": now.isoformat(), "price_per_unit_usd": 1.62, "source": "csv_replay", "ingested_at": ingested_at},
]

# 3. Route Weather
route_weather_rows = [
    {
        "container_id": "CONT-A",
        "vessel_mmsi": "111111111",
        "latitude": 28.50,
        "longitude": -116.20,
        "observed_at": now.isoformat(),
        "ambient_temp_c": 19.4,
        "ambient_humidity_pct": 74.0,
        "wind_speed_kmh": 22.5,
        "precipitation_mm": 0.0,
        "wave_height_m": 2.1,
        "sea_surface_temp_c": 18.2,
        "ingested_at": ingested_at
    },
    {
        "container_id": "CONT-B",
        "vessel_mmsi": "222222222",
        "latitude": 30.10,
        "longitude": -117.80,
        "observed_at": now.isoformat(),
        "ambient_temp_c": 18.1,
        "ambient_humidity_pct": 71.0,
        "wind_speed_kmh": 18.0,
        "precipitation_mm": 0.0,
        "wave_height_m": 1.8,
        "sea_surface_temp_c": 17.5,
        "ingested_at": ingested_at
    }
]

# 4. Container Telemetry with 24 hours history
sig = pd.read_csv("data/processed/door_open_signature.csv")
sig_t = sig["t_rel_s"].to_numpy()
sig_dt = sig["delta_temp_c"].to_numpy()
sig_dh = sig["delta_humidity_pct"].to_numpy()
sig_len = float(sig_t[-1])

telemetry_rows = []
# Generate 24 hours history at 15-minute intervals (96 intervals)
start_time = now - timedelta(hours=24)
fault_start = start_time + timedelta(hours=12) # CONT-A refrigeration starts drifting at 12 hours ago

for step in range(97):
    t_step = start_time + timedelta(minutes=step * 15)
    
    # CONT-A: drifts up to ~13.5 C
    drift_hours = max(0.0, (t_step - fault_start).total_seconds() / 3600.0) if t_step >= fault_start else 0.0
    temp_a = round(6.0 + 0.6 * drift_hours + random.gauss(0, 0.05), 2)
    hum_a = round(min(100.0, max(0.0, 90.0 + random.gauss(0, 0.3))), 1)
    
    # Door open event at step 70 for CONT-A
    door_a = (step == 70)
    if door_a:
        temp_a += 1.8
        hum_a += 8.0
        
    telemetry_rows.append({
        "container_id": "CONT-A",
        "vessel_mmsi": "111111111",
        "commodity": "avocado",
        "observed_at": t_step.isoformat(),
        "internal_temp_c": temp_a,
        "internal_humidity_pct": hum_a,
        "setpoint_temp_c": 6.0,
        "door_open": door_a,
        "ingested_at": ingested_at
    })
    
    # CONT-B: optimal around 6.0 C
    temp_b = round(6.0 + random.gauss(0, 0.05), 2)
    hum_b = round(min(100.0, max(0.0, 90.0 + random.gauss(0, 0.3))), 1)
    door_b = (step == 45)
    if door_b:
        temp_b += 1.5
        hum_b += 6.0
        
    telemetry_rows.append({
        "container_id": "CONT-B",
        "vessel_mmsi": "222222222",
        "commodity": "avocado",
        "observed_at": t_step.isoformat(),
        "internal_temp_c": temp_b,
        "internal_humidity_pct": hum_b,
        "setpoint_temp_c": 6.0,
        "door_open": door_b,
        "ingested_at": ingested_at
    })

# Load batches to BigQuery
tables_to_load = [
    ("raw_vessel_position", vessel_rows),
    ("raw_market_price", market_rows),
    ("raw_route_weather", route_weather_rows),
    ("raw_container_telemetry", telemetry_rows),
]

for tbl, rows in tables_to_load:
    tbl_id = f"{PROJECT_ID}.weather_raw.{tbl}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, tbl_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows into {tbl}")

print("Seeding complete!")
