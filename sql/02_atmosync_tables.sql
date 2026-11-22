-- =============================================================================
-- AtmoSync Raw Ingestion Tables for Container Telemetry, Vessel, Route Weather & Market Prices
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw.raw_container_telemetry` (
  container_id          STRING    NOT NULL,
  vessel_mmsi           STRING,
  commodity             STRING,
  observed_at           TIMESTAMP NOT NULL,
  internal_temp_c       FLOAT64,
  internal_humidity_pct FLOAT64,
  setpoint_temp_c       FLOAT64,
  door_open             BOOL,
  ingested_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY container_id;

CREATE TABLE IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw.raw_vessel_position` (
  mmsi         STRING    NOT NULL,
  ship_name    STRING,
  latitude     FLOAT64,
  longitude    FLOAT64,
  sog_knots    FLOAT64,
  cog_deg      FLOAT64,
  source       STRING,
  observed_at  TIMESTAMP NOT NULL,
  ingested_at  TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY mmsi;

CREATE TABLE IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw.raw_route_weather` (
  container_id         STRING    NOT NULL,
  vessel_mmsi          STRING,
  latitude             FLOAT64,
  longitude            FLOAT64,
  observed_at          TIMESTAMP NOT NULL,
  ambient_temp_c       FLOAT64,
  ambient_humidity_pct FLOAT64,
  wind_speed_kmh       FLOAT64,
  precipitation_mm     FLOAT64,
  wave_height_m        FLOAT64,
  sea_surface_temp_c   FLOAT64,
  ingested_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY container_id;

CREATE TABLE IF NOT EXISTS `ferrous-coda-515405-q2.weather_raw.raw_market_price` (
  port_id             STRING    NOT NULL,
  price_region        STRING,
  observed_at         TIMESTAMP NOT NULL,
  price_per_unit_usd  FLOAT64,
  source              STRING,
  ingested_at         TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY port_id;
