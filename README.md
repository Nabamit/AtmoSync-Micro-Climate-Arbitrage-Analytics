# Real-Time Weather Data Pipeline (Kafka + BigQuery + dbt + Streamlit / Power BI)

An end-to-end data engineering and analytics project that collects live weather data from the free [Open-Meteo API](https://open-meteo.com/), streams it through **Apache Kafka**, loads it into **Google BigQuery**, transforms it with **dbt**, and visualizes it with **two alternative dashboards**:

- **Plan A:** a Python dashboard built with **Streamlit**
- **Plan B:** a **Power BI** dashboard connected to BigQuery

> Weather data by [Open-Meteo.com](https://open-meteo.com/) (licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)).

> **Verified Nov 2026 (re-check the linked Google and Microsoft docs, because these details change):**
> - The **BigQuery sandbox** needs no credit card and gives the free-tier limits (10 GB storage, 1 TB of queries per month), but **tables expire after 60 days** and **streaming inserts, DML statements, and the Data Transfer Service are not available**.
> - Because of that, this project loads data with **batch load jobs** (free, and allowed in the sandbox) instead of streaming inserts, and dbt uses only **views and tables** (no incremental models, which use `MERGE`).
> - Power BI's BigQuery connector supports sign-in with a Google account or a Google service account.

---

## Table of Contents

1. [Project Purpose](#1-project-purpose)
2. [The Two Dashboard Plans](#2-the-two-dashboard-plans)
3. [Basic Fundamentals](#3-basic-fundamentals)
4. [Architecture](#4-architecture)
5. [Tools & Technologies](#5-tools--technologies)
6. [Project Structure](#6-project-structure)
7. [Prerequisites](#7-prerequisites)
8. [Phase 1: Build the Data Pipeline](#8-phase-1-build-the-data-pipeline)
9. [Phase 2, Plan A: Streamlit Dashboard](#9-phase-2-plan-a-streamlit-dashboard)
10. [Phase 2, Plan B: Power BI Dashboard](#10-phase-2-plan-b-power-bi-dashboard)
11. [Scheduling](#11-scheduling)
12. [Cost Control & Limits](#12-cost-control--limits)
13. [Troubleshooting](#13-troubleshooting)
14. [API Limits & Attribution](#14-api-limits--attribution)
15. [Future Improvements](#15-future-improvements)

---

## 1. Project Purpose

**What is this project meant to be?**

A hands-on portfolio project that demonstrates a modern cloud data stack from source to dashboard:

- **Ingest** live data from a public REST API
- **Stream** it using a message broker (Kafka)
- **Load** it into a serverless cloud data warehouse (BigQuery)
- **Transform** it into clean, analysis-ready tables (dbt)
- **Visualize** it two ways: a code-based dashboard (Streamlit) and a BI-tool dashboard (Power BI)

**Skills it shows:** API integration, streaming concepts, cloud data warehousing, SQL, data modeling, analytics engineering, cloud IAM and authentication, Python dashboard development, BI dashboard design, and pipeline automation.

**Questions the dashboards answer:**

- What is the current temperature, humidity, and wind speed in each city?
- How do temperatures trend over the last hours and days?
- Which city is hottest, coldest, or windiest right now?
- Is the pipeline fresh (when was the last update)?

---

## 2. The Two Dashboard Plans

Phase 1 (the data pipeline) is identical for both plans. You only choose how to present the data in Phase 2. You can build both.

| | **Plan A: Streamlit** | **Plan B: Power BI** |
|---|---|---|
| Built with | Python code (`app.py`) | Drag-and-drop in Power BI Desktop |
| Data refresh | Automatic (cached query, refreshes every 5 minutes) | Manual or scheduled refresh in Power BI |
| Runs on | Windows, macOS, Linux | Power BI Desktop is Windows only |
| Sharing | Live link if deployed (Streamlit Community Cloud) | `.pbix` file, screenshots, or Power BI Service |
| Cost | Free | Power BI Desktop is free |
| Skills it highlights | Python, pandas, app development | BI modeling, visual design, business reporting |
| Auth to BigQuery | Your Google credentials locally, or a read-only service account when deployed | Google account sign-in, or a read-only service account |

---

## 3. Basic Fundamentals

| Concept | Plain-English meaning | Role in this project |
|---|---|---|
| **REST API** | A web address you request (HTTP GET) to receive data, usually as JSON. | Open-Meteo returns weather as JSON. No API key needed. |
| **Polling** | Asking an API repeatedly on a schedule. | Open-Meteo doesn't push data, so our producer asks every 15 minutes. |
| **Message broker** | A middleman that safely holds messages between programs. | Kafka sits between the API fetcher and the warehouse loader. |
| **Producer / Consumer** | Producer sends messages to a topic. Consumer reads them. | Producer fetches weather. Consumer loads it into BigQuery. |
| **Topic / Offset** | A named channel in Kafka / a bookmark of what's been read. | `weather_raw` topic. Offsets are committed only after a successful load. |
| **BigQuery** | Google's serverless data warehouse. No servers to manage, you pay per query by data scanned. | Stores raw and transformed weather data. |
| **Project → Dataset → Table** | BigQuery's hierarchy: `project.dataset.table`. A dataset is like a folder of tables. | `weather_raw` and `weather_analytics` datasets. |
| **Sandbox / free tier** | Free usage: 10 GB storage and 1 TB of queries per month. The sandbox needs no card but expires tables after 60 days. | Lets you build the whole project at no cost. |
| **Load job vs streaming insert** | A load job adds a batch of rows (free). Streaming inserts add rows one by one (billed, and not available in the sandbox). | Consumer uses batch load jobs. |
| **Partitioning & clustering** | Organizing a table so queries scan less data. | Raw table is partitioned by load date and clustered by city. |
| **IAM, service account, ADC** | Permissions are granted to roles. A service account is a robot identity. Application Default Credentials (ADC) lets your code reuse your `gcloud` login. | Local scripts use ADC or Service Account JSON key. Dashboards can use a read-only service account. |
| **ELT** | Load raw data first, then transform inside the warehouse. | Raw table → dbt models, all inside BigQuery. |
| **dbt** | Runs SQL `SELECT` models in your warehouse to build clean tables, with tests and docs. | Builds staging and mart tables. |
| **Layered modeling** | Raw → Staging (clean) → Marts (business-ready). | `raw_weather` → `stg_weather` → `fct_*` tables. |
| **Idempotency / dedup** | Re-running a step must not create bad duplicates. | BigQuery doesn't enforce unique keys, so dbt removes duplicates with `QUALIFY ROW_NUMBER()`. |
| **Streamlit rerun model** | A Streamlit script reruns top to bottom on every interaction. | `st.cache_data` caches query results so BigQuery isn't queried on every click. |
| **Import vs DirectQuery** | Import copies data into Power BI. DirectQuery queries the source live on each interaction. | Plan B uses Import (cheaper and faster). |

> **Honest note:** Kafka and BigQuery are more than this small data volume strictly needs. They are used here to learn and demonstrate a real production-style stack.

---

## 4. Architecture

```
┌──────────────────┐   HTTP GET     ┌──────────────────┐
│  Open-Meteo API  │ ─────────────► │  Python Producer │
│  (free, no key)  │   every 15 min │  (requests)      │
└──────────────────┘                └────────┬─────────┘
                                             │ JSON messages
                                             ▼
                                   ┌──────────────────┐
                                   │   Apache Kafka   │  (Docker, local)
                                   │ topic:           │
                                   │  weather_raw     │
                                   └────────┬─────────┘
                                            │
                                            ▼
                                   ┌──────────────────┐
                                   │  Python Consumer │  micro-batches
                                   │ (BigQuery load   │  (20 sec / 50 rows)
                                   │  jobs, auth)     │
                                   └────────┬─────────┘
                                            │ batch load job (free)
                                            ▼
┌───────────────────────────────────────────────────────────────┐
│                  GOOGLE BIGQUERY  (cloud)                     │
│                                                               │
│  <project>.weather_raw.raw_weather   (append-only raw data)   │
│          │                                                    │
│          │  dbt run                                           │
│          ▼                                                    │
│  <project>.weather_analytics                                  │
│     stg_weather        (view: clean + dedupe)                 │
│     fct_weather_hourly (table)                                │
│     fct_weather_daily  (table)                                │
│     fct_weather_latest (table: newest reading per city)       │
└───────────────┬───────────────────────────────┬───────────────┘
                │                               │
                ▼                               ▼
   ┌─────────────────────────┐     ┌─────────────────────────┐
   │  PLAN A: Streamlit      │     │  PLAN B: Power BI       │
   │  Python app, cached     │     │  BigQuery connector,    │
   │  queries, KPI cards,    │     │  Import mode, visuals,  │
   │  charts, map            │     │  refresh                │
   └─────────────────────────┘     └─────────────────────────┘
```

**Data flow summary**

1. The producer calls the API for each configured city and publishes one JSON message per city to Kafka.
2. The consumer collects messages into small batches and loads each batch into `raw_weather` with a BigQuery load job.
3. dbt cleans, de-duplicates, and aggregates the raw data into mart tables in `weather_analytics`.
4. **Plan A:** Streamlit queries the mart tables and renders the dashboard. **Plan B:** Power BI imports the mart tables and renders the dashboard.

---

## 5. Tools & Technologies

| Layer | Tool | Purpose |
|---|---|---|
| Data source | [Open-Meteo API](https://open-meteo.com/en/docs) | Free weather forecast and current conditions |
| Language | Python 3.10+ | Producer, consumer, and Streamlit app |
| HTTP client | `requests` | Calls the API |
| Streaming | Apache Kafka (KRaft mode, Docker) | Message broker |
| Kafka client | `kafka-python-ng` / `confluent-kafka` | Python producer and consumer |
| Data warehouse | **Google BigQuery** (sandbox or free tier) | Cloud storage and SQL engine |
| Google tooling | Google Cloud Console / Service Account / `gcloud` | Cloud setup, IAM, BigQuery datasets |
| BigQuery client | `google-cloud-bigquery`, `pandas`, `pyarrow`, `db-dtypes` | Consumer loads data. Streamlit reads data. |
| Transformation | dbt Core + `dbt-bigquery` | SQL modeling, tests, docs |
| **Dashboard (Plan A)** | **Streamlit** + pandas | Python web dashboard |
| **Dashboard (Plan B)** | **Power BI Desktop** (Windows) | BI dashboard |
| Containers | Docker + Docker Compose | Runs Kafka locally |
| Config | `python-dotenv` | Loads `.env` settings |
| Scheduling | Windows Task Scheduler / cron | Runs dbt on a schedule |
| Deployment (optional) | Streamlit Community Cloud | Public link for Plan A |

---

## 6. Project Structure

```
weather-pipeline/
├── README.md
├── docker-compose.yml            # Kafka only (BigQuery is in the cloud)
├── requirements.txt
├── .env.example
├── .gitignore
├── sql/
│   └── 01_bigquery_setup.sql     # datasets + raw table
├── ingestion/
│   ├── config.py                 # cities, Kafka topic, BigQuery settings
│   ├── producer.py               # API → Kafka
│   └── consumer.py               # Kafka → BigQuery (batch load jobs)
├── weather_dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── sources.yml
│       ├── schema.yml            # dbt tests
│       ├── staging/
│       │   └── stg_weather.sql
│       └── marts/
│           ├── fct_weather_hourly.sql
│           ├── fct_weather_daily.sql
│           └── fct_weather_latest.sql
├── dashboard/
│   └── app.py                    # Plan A: Streamlit dashboard
└── .streamlit/
    └── secrets.toml.example      # Plan A: service account template (deployment)
```

---

## 7. Prerequisites

- A Google account (for free BigQuery Sandbox)
- Docker Desktop installed and running
- Python 3.10 or newer
- Git
- **Plan B only:** Power BI Desktop (Windows)

---

## 8. Phase 1: Build the Data Pipeline

### Step 1: Create a Google Cloud project and open BigQuery

1. Sign in at [console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery) with your Google account.
2. Create a new project (for example `weather-pipeline`). Note the **Project ID** (it may differ from the name).
3. BigQuery Sandbox is automatically enabled with zero credit card needed (10 GB storage, 1 TB queries free per month).

### Step 2: Authentication

- **Service Account (Recommended on Windows):** Create a service account in Cloud IAM with `BigQuery Admin` (or `BigQuery Data Editor` + `BigQuery Job User`), download the JSON key as `service-account-key.json`, and reference it in `.env`.
- **Google Cloud SDK / ADC:** Alternatively, install `gcloud` and run `gcloud auth application-default login`.

### Step 3: Create datasets and raw table

In the BigQuery console, run `sql/01_bigquery_setup.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.weather_raw`
  OPTIONS (location = 'US');

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.weather_analytics`
  OPTIONS (location = 'US');

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.weather_raw.raw_weather` (
  city              STRING    NOT NULL,
  latitude          FLOAT64,
  longitude         FLOAT64,
  observed_at       DATETIME  NOT NULL,
  temperature_c     FLOAT64,
  humidity_pct      FLOAT64,
  wind_speed_kmh    FLOAT64,
  precipitation_mm  FLOAT64,
  weather_code      INT64,
  ingested_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY city;
```

### Step 4: Python environment and project files

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Step 5: Start Kafka with Docker

```bash
docker compose up -d
docker ps
```

### Step 6 & 7: Run Ingestion

```bash
# Terminal 1: Consumer (Kafka -> BigQuery)
python ingestion/consumer.py

# Terminal 2: Producer (Open-Meteo -> Kafka)
python ingestion/producer.py
```

### Step 8: Transform with dbt

```bash
cd weather_dbt
dbt debug
dbt run
dbt test
```

---

## 9. Phase 2, Plan A: Streamlit Dashboard

Run the Streamlit application:
```bash
streamlit run dashboard/app.py
```

---

## 10. Phase 2, Plan B: Power BI Dashboard

1. Open **Power BI Desktop** → **Get Data** → **Google BigQuery**.
2. Sign in with your Google Account or Service Account.
3. Import the transformed tables: `fct_weather_latest`, `fct_weather_hourly`, and `fct_weather_daily`.
4. Build the visuals (KPI Cards, Temperature Line Chart, Daily Bar Chart, and Map).

---

## 11. Cost Control & Limits

- **Sandbox:** No credit card required. 10 GB storage and 1 TB queries free per month.
- **Batch Load Jobs:** 100% free (streaming inserts are billed and disabled in sandbox).
- **Streamlit Cache:** Query cached with `st.cache_data(ttl=300)` and safety cap `maximum_bytes_billed`.

---

## License & Credits

- Weather data: [Open-Meteo.com](https://open-meteo.com/), CC BY 4.0
- Pipeline code: MIT
