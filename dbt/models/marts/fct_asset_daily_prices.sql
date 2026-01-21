{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'dt_session_date']
) }}

with 

source as (

    select 
        desc_ticker,
        dt_session_date,
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
        audit_loaded_at 
    
    from {{ ref('int_price_technicals_base') }}

    {% if is_incremental() %}
        where dt_session_date :: timestamp > (select coalesce(max(dt_session_date), '1970-01-01' :: timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        dt_session_date,
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
        audit_loaded_at, 
        current_timestamp() :: timestamp as audit_created_at

    from source
)

select * from renamed