with readings as (
    select
        t.container_id, t.vessel_mmsi, t.commodity, t.observed_at,
        t.internal_temp_c, t.internal_humidity_pct, t.setpoint_temp_c, t.door_open,
        p.ref_temp_c, p.q10, p.base_shelf_life_days, p.rh_min, p.rh_max,
        timestamp_diff(
            t.observed_at,
            lag(t.observed_at) over (partition by t.container_id order by t.observed_at),
            second
        ) / 3600.0 as interval_hours
    from {{ ref('stg_container_telemetry') }} t
    join {{ ref('commodity_params') }} p using (commodity)
),

rates as (
    select
        *,
        -- spoilage speeds up by a factor of q10 for every +10 C above the ideal temperature
        greatest(1.0, pow(q10, (internal_temp_c - ref_temp_c) / 10.0)) as temp_factor,
        -- extra spoilage when humidity is outside the ideal band
        if(internal_humidity_pct < rh_min or internal_humidity_pct > rh_max, 1.25, 1.0) as humidity_factor
    from readings
),

steps as (
    select
        *,
        temp_factor * humidity_factor as spoilage_rate_multiplier,
        -- cap a single gap at 1 hour so a data outage does not count as a huge jump
        least(coalesce(interval_hours, 0), 1.0) * temp_factor * humidity_factor
            / (base_shelf_life_days * 24.0) as shelf_life_used_step
    from rates
)

select
    container_id, vessel_mmsi, commodity, observed_at,
    internal_temp_c, internal_humidity_pct, setpoint_temp_c, door_open,
    round(spoilage_rate_multiplier, 3) as spoilage_rate_multiplier,
    base_shelf_life_days,
    sum(shelf_life_used_step) over (partition by container_id order by observed_at) as shelf_life_used_fraction,
    greatest(
        0.0,
        1.0 - sum(shelf_life_used_step) over (partition by container_id order by observed_at)
    ) as quality_score
from steps
