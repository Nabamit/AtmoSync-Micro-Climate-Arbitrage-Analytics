import os
import csv
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Base Paths
ROOT = Path(__file__).resolve().parent.parent
SEEDS = ROOT / "weather_dbt" / "seeds"
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
STATE = ROOT / "state"
STATE.mkdir(exist_ok=True)
DATA_RAW.mkdir(exist_ok=True, parents=True)
DATA_PROCESSED.mkdir(exist_ok=True, parents=True)

# Kafka Settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", str(15 * 60)))

# AtmoSync Kafka Topics
TOPIC_TELEMETRY = "container_telemetry"
TOPIC_VESSEL = "vessel_position"
TOPIC_ROUTE_WEATHER = "route_weather"
TOPIC_MARKET = "market_price"

# APIs
API_URL = "https://api.open-meteo.com/v1/forecast"
MARINE_API_URL = "https://marine-api.open-meteo.com/v1/marine"

# BigQuery Settings
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ferrous-coda-515405-q2")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET", "weather_raw")
BQ_RAW_TABLE = os.getenv("BQ_RAW_TABLE", "raw_weather")
BQ_ANALYTICS_DATASET = os.getenv("BQ_ANALYTICS_DATASET", "weather_analytics")

# Legacy Cities
CITIES = {
    "Kolkata":   (22.5726, 88.3639),
    "Delhi":     (28.6139, 77.2090),
    "Mumbai":    (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai":   (13.0827, 80.2707),
}


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_shipments():
    return _read_csv(SEEDS / "shipments.csv")


def load_ports():
    return _read_csv(SEEDS / "ports.csv")


def load_params():
    return {row["commodity"]: row for row in _read_csv(SEEDS / "commodity_params.csv")}
