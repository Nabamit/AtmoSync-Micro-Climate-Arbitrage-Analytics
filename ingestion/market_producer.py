import os
import random
import time
from datetime import datetime, timezone

import pandas as pd

import config
import kafka_utils

CSV_PATH = config.DATA_RAW / os.getenv("AVOCADO_CSV", "avocado-updated-2020.csv")
TICK_SECONDS = 15 * 60
WEEK_EVERY_TICKS = 4          # one CSV week per hour


def key(s):
    return str(s).replace(" ", "").lower()


def load_series():
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={"averageprice": "average_price", "region": "geography"})
    df["date"] = pd.to_datetime(df["date"])
    df["geo_key"] = df["geography"].map(key)
    df = df[df["type"].str.lower() == "conventional"].sort_values("date")
    return {k: g["average_price"].tolist() for k, g in df.groupby("geo_key")}


def run():
    series = load_series()
    ports = [p for p in config.load_ports() if p["price_region"]]
    for p in ports:
        if key(p["price_region"]) not in series:
            print(f"WARNING: region {p['price_region']} not found in CSV. Check geography names.")
    start_idx = random.randint(0, min(40, len(list(series.values())[0]) - 1))
    tick = 0
    while True:
        week = start_idx + tick // WEEK_EVERY_TICKS
        now = datetime.now(timezone.utc).isoformat()
        for p in ports:
            prices = series.get(key(p["price_region"]))
            if not prices:
                continue
            base = prices[week % len(prices)]
            kafka_utils.send(config.TOPIC_MARKET, p["port_id"], {
                "port_id": p["port_id"],
                "price_region": p["price_region"],
                "observed_at": now,
                "price_per_unit_usd": round(base + random.gauss(0, 0.01), 3),
                "source": "csv_replay",
            })
        kafka_utils.flush()
        print(f"Market tick {tick} (CSV week index {week})")
        tick += 1
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    run()
