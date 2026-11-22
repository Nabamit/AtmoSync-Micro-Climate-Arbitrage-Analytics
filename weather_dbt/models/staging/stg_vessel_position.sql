select
    mmsi, ship_name, latitude, longitude, sog_knots, cog_deg, source, observed_at, ingested_at
from {{ source('weather', 'raw_vessel_position') }}
where latitude is not null
  and longitude is not null
  and (sog_knots is null or sog_knots < 60)     -- AIS uses 102.3 knots to mean "not available"
qualify row_number() over (partition by mmsi, observed_at order by ingested_at desc) = 1
