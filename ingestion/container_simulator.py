import os
import random
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import config
import kafka_utils

SAMPLE_SECONDS = 15
BACKFILL_HOURS = float(os.getenv("SIM_BACKFILL_HOURS", "24"))
DOOR_EVENTS_PER_HOUR = 0.4
DOOR_HUMIDITY_SCALE = 0.5      # lab humidity swings are large; scale down for a container
FAULT_STARTS_AT = 0.6          # fault begins 60% into the backfill window

sig = pd.read_csv(config.DATA_PROCESSED / "door_open_signature.csv")
SIG_T = sig["t_rel_s"].to_numpy()
SIG_DT = sig["delta_temp_c"].to_numpy()
SIG_DH = sig["delta_humidity_pct"].to_numpy()
SIG_LEN = float(SIG_T[-1])


class Container:
    def __init__(self, shipment, params):
        self.id = shipment["container_id"]
        self.mmsi = str(shipment["vessel_mmsi"])
        self.commodity = shipment["commodity"]
        p = params[self.commodity]
        self.setpoint = float(p["ref_temp_c"])
        self.rh_target = (float(p["rh_min"]) + float(p["rh_max"])) / 2.0
        self.drift = float(shipment.get("sim_drift_c_per_hour") or 0.0)
        self.fault_start = None
        self.door_start = None
        self.door_scale = 1.0
        self.noise_t = 0.0
        self.noise_h = 0.0

    def step(self, now):
        # Sensor noise (tune to the std printed by prepare_door_signature.py)
        self.noise_t = 0.9 * self.noise_t + random.gauss(0, 0.05)
        self.noise_h = 0.9 * self.noise_h + random.gauss(0, 0.3)
        temp = self.setpoint + self.noise_t
        hum = self.rh_target + self.noise_h

        # Refrigeration fault: temperature drifts upward
        if self.drift and self.fault_start and now >= self.fault_start:
            temp += self.drift * (now - self.fault_start).total_seconds() / 3600.0

        # Door events: random start, then replay the real event shape
        if self.door_start is None and random.random() < DOOR_EVENTS_PER_HOUR * SAMPLE_SECONDS / 3600.0:
            self.door_start = now
            self.door_scale = random.uniform(0.8, 1.6)

        door_open = False
        if self.door_start is not None:
            elapsed = (now - self.door_start).total_seconds()
            if elapsed > SIG_LEN:
                self.door_start = None
            else:
                temp += self.door_scale * float(np.interp(elapsed, SIG_T, SIG_DT))
                hum += DOOR_HUMIDITY_SCALE * self.door_scale * float(np.interp(elapsed, SIG_T, SIG_DH))
                door_open = elapsed < 60          # first minute = door open

        hum = min(max(hum, 0.0), 100.0)
        return {
            "container_id": self.id,
            "vessel_mmsi": self.mmsi,
            "commodity": self.commodity,
            "observed_at": now.isoformat(),
            "internal_temp_c": round(temp, 2),
            "internal_humidity_pct": round(hum, 1),
            "setpoint_temp_c": self.setpoint,
            "door_open": door_open,
        }


def run():
    params = config.load_params()
    containers = [Container(s, params) for s in config.load_shipments()]

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=BACKFILL_HOURS)
    fault_time = start + timedelta(hours=BACKFILL_HOURS * FAULT_STARTS_AT)
    for c in containers:
        c.fault_start = fault_time if BACKFILL_HOURS > 0 else now

    # History (run this once; set SIM_BACKFILL_HOURS=0 on restarts to avoid duplicates)
    t = start
    while t < now and BACKFILL_HOURS > 0:
        for c in containers:
            kafka_utils.send(config.TOPIC_TELEMETRY, c.id, c.step(t))
        t += timedelta(seconds=SAMPLE_SECONDS)
    kafka_utils.flush()
    print("Backfill done. Switching to live mode.")

    # Live
    while True:
        now = datetime.now(timezone.utc)
        for c in containers:
            kafka_utils.send(config.TOPIC_TELEMETRY, c.id, c.step(now))
        kafka_utils.flush()
        time.sleep(SAMPLE_SECONDS)


if __name__ == "__main__":
    run()
