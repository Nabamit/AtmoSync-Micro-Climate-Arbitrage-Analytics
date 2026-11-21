select
    city,
    latitude,
    longitude,
    observed_at,
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    precipitation_mm,
    weather_code,
    ingested_at
from {{ ref('stg_weather') }}
where true   -- BigQuery requires WHERE, GROUP BY, or HAVING alongside QUALIFY
qualify row_number() over (partition by city order by observed_at desc) = 1
