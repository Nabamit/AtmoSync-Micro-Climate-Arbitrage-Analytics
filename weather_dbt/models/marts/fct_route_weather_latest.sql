select
    container_id, vessel_mmsi, latitude, longitude, observed_at,
    ambient_temp_c, ambient_humidity_pct, wind_speed_kmh, precipitation_mm,
    wave_height_m, sea_surface_temp_c
from {{ ref('stg_route_weather') }}
where true
qualify row_number() over (partition by container_id order by observed_at desc) = 1
