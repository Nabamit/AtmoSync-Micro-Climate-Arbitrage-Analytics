import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

st.set_page_config(
    page_title="Live Weather Analytics Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATASET = os.getenv("BQ_ANALYTICS_DATASET", "weather_analytics")
LOCATION = os.getenv("BQ_LOCATION", "US")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


@st.cache_resource
def get_client() -> bigquery.Client:
    """Returns an authenticated BigQuery Client via service account or ADC."""
    # 1. Streamlit Community Cloud secret check
    try:
        info = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info["project_id"], location=LOCATION)
    except Exception:
        pass

    # 2. Local Service Account JSON key check
    if KEY_PATH and os.path.exists(KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(KEY_PATH)
        return bigquery.Client(credentials=creds, project=PROJECT_ID, location=LOCATION)

    # 3. Fallback to Application Default Credentials (ADC)
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


@st.cache_data(ttl=300)  # Cache data for 5 minutes to prevent redundant scans
def run_query(sql: str) -> pd.DataFrame:
    client = get_client()
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=100 * 1024 * 1024  # 100 MB safety cap
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def table(name: str) -> str:
    client = get_client()
    return f"`{client.project}.{DATASET}.{name}`"


# ---- Header ----
st.title("🌤️ Live Weather Data Pipeline Dashboard")
st.markdown(
    "**End-to-End Stream:** Open-Meteo REST API ➔ Apache Kafka (KRaft) ➔ Google BigQuery ➔ dbt Core ➔ Streamlit"
)

try:
    latest = run_query(f"select * from {table('fct_weather_latest')} order by city")
    hourly = run_query(f"select * from {table('fct_weather_hourly')} order by hour_start")
    daily = run_query(f"select * from {table('fct_weather_daily')} order by weather_date")
except Exception as e:
    st.error(f"⚠️ Could not load data from BigQuery: {e}")
    st.info("Ensure BigQuery datasets are created, the Kafka ingestion pipeline is running, and `dbt run` has completed.")
    st.stop()

if latest.empty:
    st.warning("No data in BigQuery yet. Start the producer and consumer, then run `dbt run`.")
    st.stop()

# ---- Sidebar filters ----
st.sidebar.header("Filter Options")
cities = sorted(latest["city"].unique())
selected_cities = st.sidebar.multiselect("Select Cities", cities, default=cities)

time_window = st.sidebar.selectbox(
    "Time Range Filter",
    ["Last 24 hours", "Last 3 days", "Last 7 days", "All available data"],
    index=1
)

if st.sidebar.button("🔄 Refresh Data Now"):
    st.cache_data.clear()
    st.rerun()

if not selected_cities:
    st.sidebar.warning("Please select at least one city.")
    st.stop()

# Apply filters
latest_filtered = latest[latest["city"].isin(selected_cities)].copy()
hourly_filtered = hourly[hourly["city"].isin(selected_cities)].copy()
daily_filtered = daily[daily["city"].isin(selected_cities)].copy()

hourly_filtered["hour_start"] = pd.to_datetime(hourly_filtered["hour_start"])
daily_filtered["weather_date"] = pd.to_datetime(daily_filtered["weather_date"])

window_hours_map = {
    "Last 24 hours": 24,
    "Last 3 days": 72,
    "Last 7 days": 168
}

if time_window in window_hours_map and not hourly_filtered.empty:
    cutoff = hourly_filtered["hour_start"].max() - pd.Timedelta(hours=window_hours_map[time_window])
    hourly_filtered = hourly_filtered[hourly_filtered["hour_start"] >= cutoff]

# ---- Freshness Indicator ----
last_load = pd.to_datetime(latest_filtered["ingested_at"], utc=True).max()
minutes_ago = (pd.Timestamp.now(tz="UTC") - last_load).total_seconds() / 60
st.caption(f"⏱️ **Last Data Load:** {minutes_ago:.1f} minutes ago ({last_load.strftime('%Y-%m-%d %H:%M:%S UTC')})")

# ---- Current Conditions KPI Cards ----
st.subheader("📍 Current Weather Conditions")
kpi_cols = st.columns(len(latest_filtered))
for col, (_, row) in zip(kpi_cols, latest_filtered.iterrows()):
    col.metric(
        label=row["city"],
        value=f"{row['temperature_c']:.1f} °C",
        delta=f"{row['humidity_pct']:.0f}% Humidity",
        delta_color="off"
    )
    col.caption(f"💨 {row['wind_speed_kmh']:.1f} km/h | 🌧️ {row['precipitation_mm']:.1f} mm")

st.divider()

# ---- Visual Charts ----
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 Hourly Average Temperature (°C)")
    if not hourly_filtered.empty:
        temp_pivot = hourly_filtered.pivot_table(
            index="hour_start",
            columns="city",
            values="avg_temperature_c"
        )
        st.line_chart(temp_pivot)
    else:
        st.info("No hourly trend data for the selected range.")

with col_right:
    st.subheader("📊 Daily Maximum Temperature Extremes (°C)")
    if not daily_filtered.empty:
        daily_pivot = daily_filtered.pivot_table(
            index="weather_date",
            columns="city",
            values="max_temperature_c"
        )
        st.bar_chart(daily_pivot)
    else:
        st.info("No daily extreme data available.")

# ---- Map ----
st.subheader("🗺️ Monitored City Locations")
st.map(latest_filtered, latitude="latitude", longitude="longitude")

# ---- Raw Table View ----
with st.expander("🔍 View Latest Curated Telemetry Table"):
    st.dataframe(latest_filtered, use_container_width=True)

st.caption(
    "Data Source: [Open-Meteo.com](https://open-meteo.com/) (Licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/))"
)
