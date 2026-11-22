select port_id, price_region, observed_at, price_per_unit_usd, source
from {{ ref('stg_market_price') }}
where true
qualify row_number() over (partition by port_id order by observed_at desc) = 1
