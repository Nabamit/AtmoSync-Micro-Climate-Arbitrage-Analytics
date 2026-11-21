select
    city,
    date(observed_at)                  as weather_date,
    round(avg(temperature_c), 1)       as avg_temperature_c,
    min(temperature_c)                 as min_temperature_c,
    max(temperature_c)                 as max_temperature_c,
    round(avg(humidity_pct), 1)        as avg_humidity_pct,
    round(avg(wind_speed_kmh), 1)      as avg_wind_speed_kmh,
    round(sum(precipitation_mm), 2)    as total_precipitation_mm
from {{ ref('stg_weather') }}
group by city, weather_date
