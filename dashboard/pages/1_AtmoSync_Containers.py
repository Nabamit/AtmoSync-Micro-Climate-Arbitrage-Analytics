import os

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
st.set_page_config(page_title="AtmoSync: Spoilage Arbitrage", page_icon="🥑", layout="wide")

DATASET = os.getenv("BQ_ANALYTICS_DATASET", "weather_analytics")
LOCATION = os.getenv("BQ_LOCATION", "US")
STATUS_ICON = {"OK": "🟢", "WARNING": "🟠", "CRITICAL": "🔴"}
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account-key.json")


@st.cache_resource
def get_client():
    try:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info["project_id"], location=LOCATION)
    except Exception:
        pass

    if KEY_PATH and os.path.exists(KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(KEY_PATH)
        return bigquery.Client(credentials=creds, project=os.getenv("GCP_PROJECT_ID"), location=LOCATION)
    return bigquery.Client(project=os.getenv("GCP_PROJECT_ID"), location=LOCATION)


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=200 * 1024 * 1024)
    return get_client().query(sql, job_config=job_config).to_dataframe()


def T(name: str) -> str:
    return f"`{get_client().project}.{DATASET}.{name}`"


st.title("🥑 AtmoSync: Container Micro-Climate and Spoilage Arbitrage")
st.caption(
    "Container sensor data is simulated. Weather, sea conditions and ship positions come "
    "from live APIs; prices are a replay of real historical prices."
)

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

try:
    latest = q(f"select * from {T('fct_container_latest')} order by container_id")
    arb = q(f"select * from {T('fct_spoilage_arbitrage')} order by spoilage_arbitrage_usd desc")
    vessels = q(f"select * from {T('fct_vessel_latest')}")
    weather = q(f"select * from {T('fct_route_weather_latest')}")
    hist = q(f"""
        select container_id, observed_at, internal_temp_c, internal_humidity_pct,
               setpoint_temp_c, door_open, quality_score
        from {T('fct_container_spoilage')}
        where observed_at >= timestamp_sub(current_timestamp(), interval 24 hour)
        order by observed_at
    """)
except Exception as e:
    st.error(f"⚠️ Unable to query BigQuery tables: {e}")
    st.info("Make sure you run `dbt seed` and `dbt run` first so the AtmoSync marts exist.")
    st.stop()

if latest.empty:
    st.warning("No container data yet. Start the simulator and consumer, then run dbt.")
    st.stop()

# ---- Reroute alerts ----
st.subheader("🚨 Real-Time Arbitrage Alerts")
reroutes = arb[arb["recommendation"] == "REROUTE"] if not arb.empty else arb
if reroutes.empty:
    st.success("✅ All containers optimal. No reroute recommended right now.")
else:
    for _, r in reroutes.iterrows():
        st.error(
            f"🚨 **REROUTE ALERT:** Container **{r['container_id']}** drifting! "
            f"Reroute from **{r['primary_market']}** to **{r['secondary_market']}**. "
            f"Estimated Net Gain: **${r['spoilage_arbitrage_usd']:,.0f}**. "
            f"Quality on Arrival: **{r['secondary_quality_at_arrival']:.2f}** (vs {r['primary_quality_at_arrival']:.2f} at primary market)."
        )

# ---- Container cards ----
st.subheader("📦 Live Container Micro-Climate Status")
cols = st.columns(len(latest))
for col, (_, r) in zip(cols, latest.iterrows()):
    col.metric(
        f"{STATUS_ICON.get(r['status'], '⚪')} {r['container_id']}",
        f"{r['internal_temp_c']:.1f} °C",
        f"{r['temp_drift_c']:+.1f} °C vs setpoint",
        delta_color="inverse",
    )
    col.caption(
        f"💧 {r['internal_humidity_pct']:.0f}% RH · Quality: {r['quality_score']:.2f} · "
        f"Shelf life left: {r['shelf_life_left_days']:.1f} d · "
        f"Door opened {int(r['door_open_readings_1h'])}× last hour"
    )

st.divider()

# ---- Detail for one container ----
st.subheader("📈 24-Hour Micro-Climate & Degradation Curves")
choice = st.selectbox("Select Container for Detailed Telemetry", latest["container_id"].tolist())
h = hist[hist["container_id"] == choice].copy()
h["observed_at"] = pd.to_datetime(h["observed_at"], utc=True)
h["door_open"] = h["door_open"].fillna(False).astype(bool)

base = alt.Chart(h).encode(x=alt.X("observed_at:T", title="Time (UTC)"))
temp_line = base.mark_line().encode(
    y=alt.Y("internal_temp_c:Q", title="Internal temperature (°C)", scale=alt.Scale(zero=False))
)
setpoint = base.mark_line(strokeDash=[4, 4], color="gray").encode(y="setpoint_temp_c:Q")
doors = base.transform_filter("datum.door_open == true").mark_point(color="red", size=60).encode(
    y="internal_temp_c:Q"
)

left, right = st.columns(2)
with left:
    st.caption("Internal Temperature (Dashed = Setpoint, Red Dots = Door Open Events)")
    st.altair_chart(temp_line + setpoint + doors, use_container_width=True)
with right:
    st.caption("Internal Humidity (%) & Spoilage Quality Score")
    st.line_chart(h.set_index("observed_at")[["internal_humidity_pct"]])
    st.line_chart(h.set_index("observed_at")[["quality_score"]])

# ---- Outside conditions for this container ----
w = weather[weather["container_id"] == choice]
if not w.empty:
    w = w.iloc[0]
    st.caption(f"🌊 Outside Marine & Weather Conditions for {choice} (Vessel MMSI: {w['vessel_mmsi']})")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Outside Air Temp", f"{w['ambient_temp_c']:.1f} °C" if pd.notna(w["ambient_temp_c"]) else "n/a")
    c2.metric("Outside Air Humidity", f"{w['ambient_humidity_pct']:.0f} %" if pd.notna(w["ambient_humidity_pct"]) else "n/a")
    c3.metric("Wave Height", f"{w['wave_height_m']:.1f} m" if pd.notna(w["wave_height_m"]) else "n/a")
    c4.metric("Sea Surface Temp", f"{w['sea_surface_temp_c']:.1f} °C" if pd.notna(w["sea_surface_temp_c"]) else "n/a")

# ---- Map and arbitrage table ----
st.subheader("🚢 Vessel Fleet Positions")
if not vessels.empty:
    st.map(vessels, latitude="latitude", longitude="longitude")

st.subheader("💰 Spoilage Arbitrage Matrix")
st.dataframe(arb, use_container_width=True)

st.caption(
    "Weather and sea data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0). "
    "Ship positions by AISstream.io when AIS_MODE=live."
)
