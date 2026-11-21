import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", str(15 * 60)))  # Default: 15 minutes

# Open-Meteo REST API
API_URL = "https://api.open-meteo.com/v1/forecast"
CITIES = {
    "Kolkata":   (22.5726, 88.3639),
    "Delhi":     (28.6139, 77.2090),
    "Mumbai":    (19.0760, 72.8777),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai":   (13.0827, 80.2707),
}

# BigQuery Settings
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET", "weather_raw")
BQ_RAW_TABLE = os.getenv("BQ_RAW_TABLE", "raw_weather")
BQ_ANALYTICS_DATASET = os.getenv("BQ_ANALYTICS_DATASET", "weather_analytics")
