import json
import time
import requests
import config

# Support both kafka-python-ng and confluent-kafka
try:
    from confluent_kafka import Producer as ConfluentProducer

    class KafkaProducerWrapper:
        def __init__(self, bootstrap_servers):
            self.producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})

        def send(self, topic, key, value):
            self.producer.produce(
                topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=json.dumps(value).encode("utf-8"),
                callback=self._on_delivery
            )

        def _on_delivery(self, err, msg):
            if err:
                print(f"[Producer] Delivery failed: {err}")
            else:
                print(f"[Producer] Sent {msg.key().decode('utf-8')} -> {msg.topic()} [partition {msg.partition()}]")

        def flush(self):
            self.producer.flush()

except ImportError:
    from kafka import KafkaProducer as PythonKafkaProducer

    class KafkaProducerWrapper:
        def __init__(self, bootstrap_servers):
            self.producer = PythonKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )

        def send(self, topic, key, value):
            future = self.producer.send(topic, key=key, value=value)
            print(f"[Producer] Sent {key} -> {topic}")
            return future

        def flush(self):
            self.producer.flush()


def fetch_city(city: str, lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code",
        "timezone": "auto",
    }
    resp = requests.get(config.API_URL, params=params, timeout=10)
    resp.raise_for_status()
    cur = resp.json()["current"]
    return {
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


def run():
    print(f"[Producer] Initializing Kafka Producer on {config.KAFKA_BOOTSTRAP}...")
    producer = KafkaProducerWrapper(config.KAFKA_BOOTSTRAP)
    print(f"[Producer] Polling Open-Meteo for {len(config.CITIES)} cities every {config.POLL_INTERVAL_SECONDS}s.")

    while True:
        for city, (lat, lon) in config.CITIES.items():
            try:
                record = fetch_city(city, lat, lon)
                producer.send(config.TOPIC, key=city, value=record)
            except Exception as e:
                print(f"[Producer] Error fetching weather for {city}: {e}")

        producer.flush()
        print(f"[Producer] Batch sent. Sleeping for {config.POLL_INTERVAL_SECONDS} seconds...")
        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
