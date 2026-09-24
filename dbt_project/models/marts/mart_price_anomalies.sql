-- Flags ticks whose price deviates more than 2 standard deviations from
-- that symbol's rolling 20-tick mean. A simple, explainable anomaly signal
-- good for demoing "data quality / monitoring" thinking in an interview.

with ticks as (

    select * from {{ ref('stg_stock_ticks') }}

),

rolling as (

    select
        *,
        avg(price) over (
            partition by symbol
            order by exchange_ts
            rows between 19 preceding and current row
        )                                                    as rolling_mean,
        stddev(price) over (
            partition by symbol
            order by exchange_ts
            rows between 19 preceding and current row
        )                                                    as rolling_stddev
    from ticks

)

select
    tick_id,
    symbol,
    exchange_ts,
    price,
    rolling_mean,
    rolling_stddev,
    case
        when rolling_stddev > 0
             and abs(price - rolling_mean) > 2 * rolling_stddev
        then true
        else false
    end                                                       as is_anomaly

from rolling
