-- Cleans and types the raw ticks landed by the consumer.
-- One row per trade tick.

with source as (

    select * from {{ source('raw', 'raw_stock_ticks') }}

),

cleaned as (

    select
        id                                         as tick_id,
        upper(trim(symbol))                        as symbol,
        price::numeric(18, 4)                       as price,
        size::integer                               as trade_size,
        exchange_timestamp::timestamptz             as exchange_ts,
        ingested_at::timestamptz                    as ingested_ts,
        consumed_at::timestamptz                    as consumed_ts,
        extract(epoch from (consumed_at - ingested_at)) * 1000
                                                     as pipeline_latency_ms

    from source
    where price > 0
      and size > 0
      and symbol is not null

)

select * from cleaned
