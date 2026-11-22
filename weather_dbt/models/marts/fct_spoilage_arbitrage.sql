{% set freight = var('freight_usd_per_kg_per_1000km', 0.05) %}
{% set threshold = var('reroute_threshold_usd', 500) %}

with shipment_state as (
    select
        s.shipment_id,
        s.container_id,
        s.quantity_kg,
        s.primary_market_id,
        s.secondary_market_id,
        v.latitude  as ship_lat,
        v.longitude as ship_lon,
        greatest(v.avg_sog_knots_1h, 5.0) * 1.852 as speed_kmh,      -- at least 5 knots
        c.quality_score,
        coalesce(c.avg_spoilage_rate_3h, c.spoilage_rate_multiplier) as spoilage_rate,
        p.base_shelf_life_days,
        p.units_per_kg,
        p.wholesale_ratio,
        p.grade_a_min_quality,
        p.grade_b_min_quality,
        p.min_sellable_quality
    from {{ ref('shipments') }} s
    join {{ ref('fct_vessel_latest') }} v on v.mmsi = cast(s.vessel_mmsi as string)
    join {{ ref('fct_container_latest') }} c on c.container_id = s.container_id
    join {{ ref('commodity_params') }} p on p.commodity = s.commodity
),

options as (
    select *, 'primary' as option_type, primary_market_id as market_id from shipment_state
    union all
    select *, 'secondary' as option_type, secondary_market_id as market_id from shipment_state
),

routed as (
    select
        o.*,
        pt.port_name as market_name,
        st_distance(
            st_geogpoint(o.ship_lon, o.ship_lat),
            st_geogpoint(pt.port_lon, pt.port_lat)
        ) / 1000.0 as distance_km
    from options o
    join {{ ref('ports') }} pt on pt.port_id = o.market_id
),

timed as (
    select
        *,
        distance_km / speed_kmh as eta_hours,
        greatest(
            0.0,
            quality_score - (distance_km / speed_kmh) * spoilage_rate / (base_shelf_life_days * 24.0)
        ) as quality_at_arrival
    from routed
),

valued as (
    select
        t.*,
        m.price_per_unit_usd,
        case
            when t.quality_at_arrival >= t.grade_a_min_quality then 1.0
            when t.quality_at_arrival >= t.grade_b_min_quality then 0.7
            when t.quality_at_arrival >= t.min_sellable_quality then 0.35
            else 0.0
        end as price_multiplier
    from timed t
    join {{ ref('fct_market_latest') }} m on m.port_id = t.market_id
),

net as (
    select
        *,
        quantity_kg * price_per_unit_usd * units_per_kg * wholesale_ratio * price_multiplier
            - quantity_kg * {{ freight }} * distance_km / 1000.0 as net_value_usd
    from valued
)

select
    p.shipment_id,
    p.container_id,
    p.market_name                          as primary_market,
    round(p.eta_hours, 1)                  as primary_eta_hours,
    round(p.quality_at_arrival, 2)         as primary_quality_at_arrival,
    round(p.net_value_usd, 0)              as primary_net_value_usd,
    s.market_name                          as secondary_market,
    round(s.eta_hours, 1)                  as secondary_eta_hours,
    round(s.quality_at_arrival, 2)         as secondary_quality_at_arrival,
    round(s.net_value_usd, 0)              as secondary_net_value_usd,
    round(s.net_value_usd - p.net_value_usd, 0) as spoilage_arbitrage_usd,
    if(s.net_value_usd - p.net_value_usd > {{ threshold }}, 'REROUTE', 'CONTINUE') as recommendation,
    current_timestamp()                    as computed_at
from net p
join net s on s.shipment_id = p.shipment_id
where p.option_type = 'primary'
  and s.option_type = 'secondary'
