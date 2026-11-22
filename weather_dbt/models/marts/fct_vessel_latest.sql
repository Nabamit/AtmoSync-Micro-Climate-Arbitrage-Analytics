with ranked as (
    select
        *,
        row_number() over (partition by mmsi order by observed_at desc) as rn
    from {{ ref('stg_vessel_position') }}
),

speed as (
    select mmsi, avg(sog_knots) as avg_sog_knots_1h
    from {{ ref('stg_vessel_position') }}
    where observed_at >= timestamp_sub(current_timestamp(), interval 1 hour)
      and sog_knots > 0.5
    group by mmsi
)

select
    r.mmsi, r.ship_name, r.latitude, r.longitude, r.sog_knots, r.cog_deg, r.source, r.observed_at,
    coalesce(s.avg_sog_knots_1h, r.sog_knots) as avg_sog_knots_1h
from ranked r
left join speed s using (mmsi)
where r.rn = 1
