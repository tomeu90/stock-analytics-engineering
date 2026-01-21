with source as (

    select
        desc_ticker,
        dt_session_date,
        mnt_close_price

    from {{ ref('int_price_technicals_base') }}

),

ema as (
    select
        *,

        avg(mnt_close_price) over (
                partition by desc_ticker
                order by dt_session_date
                rows between 11 preceding and current row
            ) :: double as mnt_exponential_moving_average_12,

        avg(mnt_close_price) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 25 preceding and current row
            ) :: double  as mnt_exponential_moving_average_26

    from source
),

macd as (

    select
        desc_ticker, 
        dt_session_date,
        round(mnt_exponential_moving_average_12 - mnt_exponential_moving_average_26, 2) :: double as rt_moving_average_convergence_divergence_line

    from ema

),

signal as (

    select
        *,
        round(avg(rt_moving_average_convergence_divergence_line) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 8 preceding and current row
        ), 2) :: double as rt_moving_average_convergence_divergence_signal

    from macd
),

histogram as (

    select
        *,
        round(rt_moving_average_convergence_divergence_line - rt_moving_average_convergence_divergence_signal, 2) :: double as rt_moving_average_convergence_divergence_histogram

    from signal

)

select * from histogram