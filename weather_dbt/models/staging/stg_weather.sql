select
    city,
    latitude,
    longitude,
    observed_at,
    round(temperature_c, 1)        as temperature_c,
    humidity_pct,
    round(wind_speed_kmh, 1)       as wind_speed_kmh,
    coalesce(precipitation_mm, 0)  as precipitation_mm,
    weather_code,
    ingested_at
from {{ source('weather', 'raw_weather') }}
where temperature_c is not null
qualify row_number() over (
    partition by city, observed_at
    order by ingested_at desc
) = 1
