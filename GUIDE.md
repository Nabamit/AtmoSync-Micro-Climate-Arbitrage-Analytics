# 📖 AtmoSync: Setup, Installation & Execution Guide

This comprehensive guide walks you step-by-step through downloading, configuring, and running the **AtmoSync Micro-Climate & Spoilage Arbitrage Analytics** platform from scratch on your local machine.

---

## 📋 1. Prerequisites & System Requirements

Before running the project, ensure you have the following software installed on your computer:

| Requirement | Minimum Version | Purpose | Download Link |
|---|---|---|---|
| **Git** | 2.30+ | Cloning repository and version control | [git-scm.com](https://git-scm.com/downloads) |
| **Python** | 3.10 – 3.14 | Running simulation producers, Kafka consumer, dbt, and Streamlit | [python.org](https://www.python.org/downloads/) |
| **Docker Desktop** | Latest | Running Apache Kafka 3.7 (KRaft mode) | [docker.com](https://www.docker.com/products/docker-desktop/) |
| **Google Cloud Account** | Free tier (Sandbox) | Cloud Data Warehouse (Google BigQuery) | [cloud.google.com](https://console.cloud.google.com/) |
| **Power BI Desktop** *(Optional)* | Latest (Windows) | Viewing the interactive `.pbix` business report (Plan B) | [powerbi.microsoft.com](https://powerbi.microsoft.com/desktop/) |

> 💡 **Cost Note:** Google BigQuery's **Sandbox mode is 100% free** and requires **no credit card**. You receive 10 GB of active storage and 1 TB of query processing every month at no cost.

---

## 📥 2. How to Download / Clone the Repository

Open your terminal (PowerShell, Command Prompt, or Bash) and run:

```bash
# Clone the repository from GitHub
git clone https://github.com/Nabamit/AtmoSync-Micro-Climate-Arbitrage-Analytics.git

# Navigate into the project root directory
cd AtmoSync-Micro-Climate-Arbitrage-Analytics
```

---

## 🐍 3. Python Virtual Environment & Dependency Installation

It is best practice to run the project inside a dedicated Python virtual environment to avoid package conflicts:

### Windows (PowerShell):
```powershell
# 1. Create a virtual environment named 'venv'
python -m venv venv

# 2. Activate the virtual environment
.\venv\Scripts\activate

# 3. Upgrade pip and install all required dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### macOS / Linux:
```bash
# 1. Create a virtual environment named 'venv'
python3 -m venv venv

# 2. Activate the virtual environment
source venv/bin/activate

# 3. Upgrade pip and install all required dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 🔑 4. Google Cloud BigQuery & Credentials Setup

AtmoSync uses Google Cloud BigQuery to store raw streaming telemetry and curate business data models.

### Step 4.1: Create a Free Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/) and sign in.
2. Click the project dropdown at the top and select **"NEW PROJECT"**.
3. Enter a project name (e.g. `atmosync-analytics`) and note down your **Project ID** (e.g., `ferrous-coda-515405-q2`).
4. Click **Create**.

### Step 4.2: Create a Service Account Key
1. In Cloud Console, open **Navigation Menu (☰)** ➔ **IAM & Admin** ➔ **Service Accounts**.
2. Click **+ CREATE SERVICE ACCOUNT**.
3. Name it `atmosync-runner` and click **Create and Continue**.
4. In the **Role** dropdown, grant:
   - **BigQuery Admin** (or *BigQuery Data Editor* + *BigQuery Job User*).
5. Click **Done**.
6. Find your new service account, click the **three dots (⋮)** on the right ➔ **Manage keys**.
7. Click **ADD KEY** ➔ **Create new key** ➔ select **JSON** ➔ click **CREATE**.
8. A `.json` file will download to your computer.
9. **Rename this file to `service-account-key.json` and move it directly into the project root directory:**
   ```text
   AtmoSync-Micro-Climate-Arbitrage-Analytics/service-account-key.json
   ```
   *(This file is automatically git-ignored to prevent credential exposure).*

> ⚠️ **Note on Organization Policy:** If you see an error saying *`iam.disableServiceAccountKeyCreation is enforced`*, open **IAM & Admin ➔ Organization Policies**, search for `Disable service account key creation`, click **Edit Policy**, set **Override parent's policy**, and set Enforcement to **Off**.

### Step 4.3: Configure `.env`
Copy `.env.example` to `.env`:
```powershell
# Windows
Copy-Item .env.example .env

# Linux / macOS
cp .env.example .env
```

Open `.env` in any text editor and fill in your GCP Project ID:
```ini
GCP_PROJECT_ID=your-gcp-project-id
BQ_LOCATION=US
BQ_RAW_DATASET=weather_raw
BQ_RAW_TABLE=raw_weather
BQ_ANALYTICS_DATASET=weather_analytics
GOOGLE_APPLICATION_CREDENTIALS=service-account-key.json

# Simulation & Voyage Settings
AISSTREAM_API_KEY=
AIS_MODE=sim
SIM_BACKFILL_HOURS=24
SIM_HOURS_AT_SEA=30
AVOCADO_CSV=avocado-updated-2020.csv
```

### Step 4.4: Initialize BigQuery Datasets & Tables
Run the automated schema creation scripts:
```powershell
# 1. Creates weather_raw and weather_analytics datasets
python scripts/setup_bigquery.py

# 2. Creates the 4 AtmoSync partitioned raw landing tables
python scripts/setup_atmosync_tables.py
```

---

## 🐳 5. Start Apache Kafka (Docker)

Make sure **Docker Desktop** is running on your machine (whale icon in the bottom-left shows green).

Start the Apache Kafka container (KRaft mode, port `9092`):
```bash
docker compose up -d
```

Verify that the container is running:
```bash
docker ps
```
*You should see the `kafka` container with status `Up`.*

---

## 🥑 6. Prepare Training Data & Physics Signatures

Before running the simulation producers, extract the calibrated door-opening signature and prepare historical market price time series:

```powershell
# Downloads Zenodo lab dataset and extracts door open temperature/humidity signature
python scripts/prepare_door_signature.py

# Prepares historical wholesale avocado prices for US destination ports
python scripts/prepare_avocado_prices.py
```

---

## ⚡ 7. How to Run the Streaming Pipeline

To run the live streaming pipeline, open separate terminal windows (with `venv` activated in each):

### Terminal 1: Multi-Topic Kafka Consumer (Warehouse Loader)
Reads from all 4 Kafka topics (`container_telemetry`, `vessel_position`, `route_weather`, `market_price`) and micro-batches records into BigQuery:
```powershell
python ingestion/consumer_atmosync.py
```

### Terminal 2: Container Telemetry Simulator
Simulates internal container sensors, door-opening events, and refrigeration cooling faults:
```powershell
python ingestion/container_simulator.py
```

### Terminal 3: Ship Position Tracker (AIS)
Streams live vessel coordinates and speed over ground (SOG):
```powershell
python ingestion/ais_producer.py
```

### Terminal 4: Route Weather & Marine Swell Producer
Fetches ambient ocean weather and wave height at the ship's current GPS location:
```powershell
python ingestion/weather_producer.py
```

### Terminal 5: Spot Market Price Producer
Streams spot prices per destination market (Seattle, Los Angeles, San Francisco):
```powershell
python ingestion/market_producer.py
```

---

## 🔄 8. Run dbt Transformations & Arbitrage Modeling

Once data begins streaming into BigQuery, run **dbt Core** to process the raw events, calculate the $Q_{10}$ shelf-life decay model, and compute the Spoilage Arbitrage recommendations:

```powershell
# 1. Load business reference seeds (commodity parameters, port coordinates, shipments)
dbt seed --project-dir weather_dbt --profiles-dir weather_dbt

# 2. Compile and execute all staging views and analytical mart tables
dbt run --project-dir weather_dbt --profiles-dir weather_dbt

# 3. Execute all 14 data quality and integrity tests
dbt test --project-dir weather_dbt --profiles-dir weather_dbt
```

---

## 📊 9. Launch the Dashboards

### 🌐 Plan A: Streamlit Interactive Web Application
Launch the live Streamlit dashboard:
```powershell
streamlit run dashboard/app.py
```
1. Open the URL printed in the terminal (usually `http://localhost:8501`).
2. In the left sidebar navigation, select **`1_AtmoSync_Containers`**.
3. View real-time **Reroute Alerts**, 24-hour **temperature drift & door opening curves**, **marine swell metrics**, **fleet map**, and the complete **Spoilage Arbitrage matrix**.

### 📈 Plan B: Power BI Desktop Report
1. Open the pre-built report in Power BI Desktop:
   ```text
   powerBI/AtmoSync-Micro-Climate-Arbitrage-Analytics.pbix
   ```
2. Click **Refresh** on the Home ribbon to pull the latest curated marts directly from your BigQuery dataset.

---

## 🛠️ 10. Common Troubleshooting

| Issue | Cause | Solution |
|---|---|---|
| **Docker named pipe error** (`failed to connect to docker API`) | Docker Desktop is not running | Launch Docker Desktop from the Start menu and wait until the engine status turns green. |
| **`DefaultCredentialsError` / BigQuery 403** | Missing or incorrect service account key | Ensure `service-account-key.json` exists in the project root and is referenced in `.env`. |
| **`FileNotFoundError: door_open_signature.csv`** | Preprocessing step was skipped | Run `python scripts/prepare_door_signature.py` to generate the file. |
| **Kafka `NoBrokersAvailable`** | Kafka container is still starting or stopped | Check `docker ps`. Run `docker compose up -d` to restart Kafka. |
| **dbt `dataset not found in location US`** | Region mismatch | Verify that `BQ_LOCATION` in `.env` and `location` in `weather_dbt/profiles.yml` match your BigQuery dataset region (default: `US`). |

---

## 📜 11. Licenses & Attribution

- **Cold Chain IoT Measurements:** Universidad Politécnica de Madrid (DOI [10.5281/zenodo.20540723](https://zenodo.org/records/20540723)), **CC BY 4.0**.
- **Atmospheric & Marine Swell Data:** [Open-Meteo.com](https://open-meteo.com/), **CC BY 4.0**.
- **Ship AIS Telemetry:** [AISstream.io](https://aisstream.io/).
- **Commodity Prices:** Hass Avocado Board data via Kaggle.
- **Pipeline Code:** MIT License.
