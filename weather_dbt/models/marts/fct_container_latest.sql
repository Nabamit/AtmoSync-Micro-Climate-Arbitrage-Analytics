with spoilage as (
    select * from {{ ref('fct_container_spoilage') }}
),

latest as (
    select * from spoilage
    where true
    qualify row_number() over (partition by container_id order by observed_at desc) = 1
),

recent as (
    select
        container_id,
        countif(door_open and observed_at >= timestamp_sub(current_timestamp(), interval 1 hour)) as door_open_readings_1h,
        avg(spoilage_rate_multiplier) as avg_spoilage_rate_3h
    from spoilage
    where observed_at >= timestamp_sub(current_timestamp(), interval 3 hour)
    group by container_id
)

select
    l.container_id, l.vessel_mmsi, l.commodity, l.observed_at,
    timestamp_diff(current_timestamp(), l.observed_at, minute) as minutes_since_last_reading,
    l.internal_temp_c, l.internal_humidity_pct, l.setpoint_temp_c,
    round(l.internal_temp_c - l.setpoint_temp_c, 2) as temp_drift_c,
    l.spoilage_rate_multiplier,
    r.avg_spoilage_rate_3h,
    l.quality_score,
    round(l.quality_score * l.base_shelf_life_days, 1) as shelf_life_left_days,
    coalesce(r.door_open_readings_1h, 0) as door_open_readings_1h,
    case
        when l.quality_score < 0.6 or abs(l.internal_temp_c - l.setpoint_temp_c) > 3 then 'CRITICAL'
        when abs(l.internal_temp_c - l.setpoint_temp_c) > 1.5 or l.spoilage_rate_multiplier > 1.5 then 'WARNING'
        else 'OK'
    end as status
from latest l
left join recent r using (container_id)
