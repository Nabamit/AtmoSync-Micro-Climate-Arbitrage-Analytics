import json
import time
from datetime import datetime, timezone

import requests

import config
import kafka_utils

POLL_SECONDS = 15 * 60


def latest_position(mmsi, origin):
    try:
        state = json.loads((config.STATE / "vessel_latest.json").read_text())
        p = state.get(str(mmsi))
        if p:
            return p["lat"], p["lon"]
    except Exception:
        pass
    return float(origin["port_lat"]), float(origin["port_lon"])


def to_utc_iso(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).isoformat()


def fetch(lat, lon):
    wx = requests.get(config.API_URL, params={
        "latitude": lat, "longitude": lon, "timezone": "UTC",
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    }, timeout=15)
    wx.raise_for_status()
    cur = wx.json()["current"]

    wave, sst = None, None
    try:
        mr = requests.get(config.MARINE_API_URL, params={
            "latitude": lat, "longitude": lon, "timezone": "UTC",
            "current": "wave_height,sea_surface_temperature",
        }, timeout=15)
        mr.raise_for_status()
        mc = mr.json().get("current", {})
        wave, sst = mc.get("wave_height"), mc.get("sea_surface_temperature")
    except Exception as e:
        print(f"Marine API unavailable for {lat:.2f},{lon:.2f} (likely land/coastal): {e}")

    return {
        "observed_at": to_utc_iso(cur["time"]),
        "ambient_temp_c": cur.get("temperature_2m"),
        "ambient_humidity_pct": cur.get("relative_humidity_2m"),
        "wind_speed_kmh": cur.get("wind_speed_10m"),
        "precipitation_mm": cur.get("precipitation"),
        "wave_height_m": wave,
        "sea_surface_temp_c": sst,
    }


def run():
    ports = {p["port_id"]: p for p in config.load_ports()}
    shipments = config.load_shipments()
    while True:
        for s in shipments:
            try:
                lat, lon = latest_position(s["vessel_mmsi"], ports[s["origin_port_id"]])
                row = fetch(lat, lon)
                row.update({
                    "container_id": s["container_id"],
                    "vessel_mmsi": str(s["vessel_mmsi"]),
                    "latitude": lat,
                    "longitude": lon,
                })
                kafka_utils.send(config.TOPIC_ROUTE_WEATHER, s["container_id"], row)
                print(f"Weather for {s['container_id']} at {lat:.2f},{lon:.2f}")
            except Exception as e:
                print(f"Weather error for {s['container_id']}: {e}")
        kafka_utils.flush()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()
