select
    container_id, vessel_mmsi, latitude, longitude, observed_at,
    ambient_temp_c, ambient_humidity_pct, wind_speed_kmh, precipitation_mm,
    wave_height_m, sea_surface_temp_c, ingested_at
from {{ source('weather', 'raw_route_weather') }}
where true
qualify row_number() over (partition by container_id, observed_at order by ingested_at desc) = 1
