with 

source as (

    select 
        desc_ticker,
        dt_session_date,
        mnt_close_price
    
    from {{ ref('int_price_technicals_base') }}

),

bollinger as (
    select
        desc_ticker,
        dt_session_date,  
        mnt_close_price,

        avg(mnt_close_price) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 19 preceding and current row
        ) as mnt_bollinger_mid_band_20,

        stddev_samp(mnt_close_price) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 19 preceding and current row
        ) as mnt_bollinger_deviation_20   

    from source   
),

bands as (
    select
        desc_ticker,
        dt_session_date,  
        round(mnt_bollinger_mid_band_20, 2) :: double as mnt_bollinger_mid_band_20,
        coalesce(round(mnt_bollinger_mid_band_20 + 2 * mnt_bollinger_deviation_20, 2), 0) :: double as mnt_bollinger_upper_band_20,
        coalesce(round(mnt_bollinger_mid_band_20 - 2 * mnt_bollinger_deviation_20, 2), 0) :: double as mnt_bollinger_lower_band_20,
        coalesce(round(try_divide((mnt_close_price - mnt_bollinger_mid_band_20), (2 * mnt_bollinger_deviation_20)), 2), 0) :: double as mnt_bollinger_zscore

    from bollinger
)

select * from bands