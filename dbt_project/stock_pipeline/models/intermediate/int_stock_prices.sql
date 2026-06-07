with staged as (
    select * from {{ ref('stg_stock_prices') }}
)

select
    symbol,
    ticker,
    event_timestamp,
    date(event_timestamp) as trade_date,
    open,
    high,
    low,
    close,
    volume,
    round(high - low, 2) as price_range,
    round((close - open) / nullif(open, 0) * 100, 4) as pct_change,
    ingested_at
from staged
