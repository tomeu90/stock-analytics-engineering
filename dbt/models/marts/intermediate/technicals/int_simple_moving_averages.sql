with 

source as (

    select 
        desc_ticker,
        dt_session_date,
        mnt_close_price
    
    from {{ ref('int_price_technicals_base') }}

),

averages as (
    select 
        *,
        round(avg(mnt_close_price) over (
                partition by desc_ticker
                order by dt_session_date
                rows between 19 preceding and current row
            ), 2) :: double as mnt_simple_moving_average_20,

        round(avg(mnt_close_price) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 49 preceding and current row
            ), 2) :: double  as mnt_simple_moving_average_50,

        round(avg(mnt_close_price) over (
            partition by desc_ticker
            order by dt_session_date
            rows between 199 preceding and current row
            ), 2) :: double  as mnt_simple_moving_average_200

    from source    
)

select * from averages