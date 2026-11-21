-- =============================================================================
-- BigQuery Setup: Datasets and Partitioned Raw Landing Table
-- Replace `YOUR_PROJECT_ID` with your actual GCP Project ID
-- =============================================================================

-- 1. Create Datasets (US multi-region location)
CREATE SCHEMA IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw`
  OPTIONS (location = 'US');

CREATE SCHEMA IF NOT EXISTS `ferrous-coda-515405-q2.weather_analytics`
  OPTIONS (location = 'US');

-- 2. Raw landing table: append-only (BigQuery Sandbox compatible)
-- Partitioned by ingestion date and clustered by city for cost-effective scanning
CREATE TABLE IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw.raw_weather` (
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
