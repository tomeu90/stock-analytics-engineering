{{ config(
    materialized='incremental',
    unique_key=['desc_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        mnt_previous_close_price,
        pct_price_gap,
        mnt_open_price,
        mnt_high_price,
        mnt_low_price,
        mnt_close_price,
        mnt_after_hours_close_price,
        qty_trades,
        qty_volume,
        qty_average_volume,
        rt_relative_volume_ratio,
        qty_after_hours_volume,
        pct_price_change,
        pct_price_change_from_open,
        pct_after_hours_price_change,
        mnt_52_week_low_price,
        mnt_52_week_high_price,
        rt_beta_coefficient, 
        rt_average_true_range, 
        pct_volatility_week, 
        pct_volatility_month, 
        pct_20_day_simple_moving_average_distance, 
        pct_50_day_simple_moving_average_distance, 
        pct_200_day_simple_moving_average_distance, 
        rt_relative_strength_index_14,
        audit_loaded_at 
    
    from {{ ref('stg_finviz__asset_attributes') }}

    {% if is_incremental() %}
        where audit_loaded_at in (
            select distinct audit_loaded_at
            from {{ ref('stg_finviz__asset_attributes') }}
            order by audit_loaded_at desc
            limit 200
        )
    {% endif %}

),

final as (
    select
        desc_ticker,
        to_timestamp(cast(audit_loaded_at as string), 'yyyyMMdd') :: timestamp as dt_session_date,
        mnt_previous_close_price,
        pct_price_gap,
        mnt_open_price,
        mnt_high_price,
        mnt_low_price,
        mnt_close_price,
        coalesce(mnt_after_hours_close_price, mnt_close_price) :: double as mnt_after_hours_close_price,
        qty_trades,
        qty_volume,
        qty_average_volume,
        rt_relative_volume_ratio,
        coalesce(qty_after_hours_volume, qty_volume) :: integer as qty_after_hours_volume,
        pct_price_change,
        pct_price_change_from_open,
        coalesce(pct_after_hours_price_change, 0) :: double as pct_after_hours_price_change,
        mnt_52_week_low_price,
        mnt_52_week_high_price,
        rt_beta_coefficient, 
        rt_average_true_range, 
        pct_volatility_week, 
        pct_volatility_month, 
        pct_20_day_simple_moving_average_distance, 
        pct_50_day_simple_moving_average_distance, 
        pct_200_day_simple_moving_average_distance, 
        rt_relative_strength_index_14,
        audit_loaded_at

    from source
)

select * from final