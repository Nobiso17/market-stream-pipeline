-- 5-minute OHLC (open/high/low/close) bars per symbol, the classic
-- candlestick aggregation used by trading dashboards.

with ticks as (

    select * from {{ ref('stg_stock_ticks') }}

),

bucketed as (

    select
        symbol,
        date_trunc('hour', exchange_ts)
            + (extract(minute from exchange_ts)::int / 5) * interval '5 minute'
                                                            as bar_start,
        price,
        trade_size,
        exchange_ts
    from ticks

),

agg as (

    select
        symbol,
        bar_start,
        bar_start + interval '5 minute'                    as bar_end,
        (array_agg(price order by exchange_ts asc))[1]      as open_price,
        max(price)                                          as high_price,
        min(price)                                          as low_price,
        (array_agg(price order by exchange_ts desc))[1]     as close_price,
        sum(trade_size)                                     as volume,
        count(*)                                            as tick_count

    from bucketed
    group by symbol, bar_start

)

select * from agg
order by symbol, bar_start
