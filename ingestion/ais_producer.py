import asyncio
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import websockets

import config
import kafka_utils

API_KEY = os.getenv("AISSTREAM_API_KEY")
MODE = os.getenv("AIS_MODE", "sim")                 # live | sim
BBOX = [[[17.0, -130.0], [49.5, -102.0]]]            # [[lat, lon], [lat, lon]]: Mexico Pacific coast to Seattle
PUBLISH_EVERY_SECONDS = 60
STATE_FILE = config.STATE / "vessel_latest.json"

_latest = {}
_last_pub = {}


def publish(mmsi, name, lat, lon, sog, cog, source):
    now = datetime.now(timezone.utc)
    _latest[str(mmsi)] = {"lat": lat, "lon": lon, "time": now.isoformat()}
    STATE_FILE.write_text(json.dumps(_latest))
    kafka_utils.send(config.TOPIC_VESSEL, mmsi, {
        "mmsi": str(mmsi),
        "ship_name": name,
        "latitude": lat,
        "longitude": lon,
        "sog_knots": sog,
        "cog_deg": cog,
        "source": source,
        "observed_at": now.isoformat(),
    })
    kafka_utils.flush()


async def run_live(discover):
    wanted = [str(s["vessel_mmsi"]) for s in config.load_shipments()]
    sub = {"APIKey": API_KEY, "BoundingBoxes": BBOX, "FilterMessageTypes": ["PositionReport"]}
    if not discover:
        sub["FiltersShipMMSI"] = wanted

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        await ws.send(json.dumps(sub))            # must be sent within 3 seconds
        seen = {}
        t0 = time.time()
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("MessageType") != "PositionReport":
                continue
            pr = msg["Message"]["PositionReport"]
            meta = msg.get("MetaData", {})
            mmsi = str(pr["UserID"])
            name = (meta.get("ShipName") or "").strip()

            if discover:
                if mmsi not in seen:
                    seen[mmsi] = name
                    print(f"MMSI {mmsi:>10}  {name:<25} speed {pr.get('Sog')} kn  "
                          f"pos {pr['Latitude']:.2f},{pr['Longitude']:.2f}")
                if time.time() - t0 > 90:
                    break
                continue

            if time.time() - _last_pub.get(mmsi, 0) < PUBLISH_EVERY_SECONDS:
                continue
            _last_pub[mmsi] = time.time()
            sog = pr.get("Sog")
            publish(mmsi, name, pr["Latitude"], pr["Longitude"], sog, pr.get("Cog"), "ais")
            print(f"Published {mmsi} {name}")


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dlmb = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def run_sim():
    """Straight-line voyage from the origin port to the primary market at 18 knots."""
    ports = {p["port_id"]: p for p in config.load_ports()}
    speed_kn = 18.0
    speed_kmh = speed_kn * 1.852
    start = datetime.now(timezone.utc) - timedelta(hours=float(os.getenv("SIM_HOURS_AT_SEA", "30")))
    voyages = []
    for s in config.load_shipments():
        o, d = ports[s["origin_port_id"]], ports[s["primary_market_id"]]
        o_lat, o_lon, d_lat, d_lon = float(o["port_lat"]), float(o["port_lon"]), float(d["port_lat"]), float(d["port_lon"])
        voyages.append((s, o_lat, o_lon, d_lat, d_lon, haversine_km(o_lat, o_lon, d_lat, d_lon)))

    while True:
        hours = (datetime.now(timezone.utc) - start).total_seconds() / 3600.0
        for s, o_lat, o_lon, d_lat, d_lon, total in voyages:
            frac = min(1.0, hours * speed_kmh / total)
            lat = o_lat + frac * (d_lat - o_lat)
            lon = o_lon + frac * (d_lon - o_lon)
            sog = 0.0 if frac >= 1.0 else speed_kn
            publish(s["vessel_mmsi"], f"SIM-{s['container_id']}", lat, lon, sog, 0.0, "sim")
        print("Published simulated positions")
        time.sleep(PUBLISH_EVERY_SECONDS)


if __name__ == "__main__":
    if MODE == "sim":
        run_sim()
    else:
        discover = "--discover" in sys.argv
        while True:
            try:
                asyncio.run(run_live(discover))
            except Exception as e:
                print(f"AIS connection problem: {e}. Retrying in 10 seconds.")
                time.sleep(10)
            if discover:
                break
