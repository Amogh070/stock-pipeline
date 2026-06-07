with int_prices as (
    select * from {{ ref('int_stock_prices') }}
)

select
    symbol,
    ticker,
    trade_date,
    min(low)                            as day_low,
    max(high)                           as day_high,
    sum(volume)                         as total_volume,
    round(avg(close), 2)                as avg_close,
    round(avg(pct_change), 4)           as avg_pct_change,
    round(avg(price_range), 2)          as avg_price_range,
    count(*)                            as tick_count
from int_prices
group by symbol, ticker, trade_date
order by trade_date desc, symbol
