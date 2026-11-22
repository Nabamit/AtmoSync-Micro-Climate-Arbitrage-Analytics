select port_id, price_region, observed_at, price_per_unit_usd, source, ingested_at
from {{ source('weather', 'raw_market_price') }}
where price_per_unit_usd is not null
qualify row_number() over (partition by port_id, observed_at order by ingested_at desc) = 1
