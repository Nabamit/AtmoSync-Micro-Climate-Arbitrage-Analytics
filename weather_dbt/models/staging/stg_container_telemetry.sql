select
    container_id, vessel_mmsi, commodity, observed_at,
    internal_temp_c, internal_humidity_pct, setpoint_temp_c, door_open, ingested_at
from {{ source('weather', 'raw_container_telemetry') }}
where internal_temp_c is not null
qualify row_number() over (
    partition by container_id, observed_at
    order by ingested_at desc
) = 1
