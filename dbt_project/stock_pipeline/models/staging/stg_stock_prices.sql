with source as (
    select * from RAW_DB.PUBLIC.STOCK_PRICES_RAW
),

cleaned as (
    select
        symbol,
        ticker,
        event_timestamp,
        open,
        high,
        low,
        close,
        volume,
        ingested_at,
        row_number() over (
            partition by symbol, event_timestamp
            order by ingested_at desc
        ) as row_num
    from source
)

select
    symbol,
    ticker,
    event_timestamp,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
from cleaned
where row_num = 1
