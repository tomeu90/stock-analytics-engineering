{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'dt_session_date']
) }}

with 

source as (

    select 
        desc_ticker,
        dt_session_date,
        rt_average_true_range, 
        pct_volatility_week, 
        pct_volatility_month, 
        pct_20_day_simple_moving_average_distance, 
        pct_50_day_simple_moving_average_distance, 
        pct_200_day_simple_moving_average_distance, 
        rt_relative_strength_index_14,
        audit_loaded_at 
    
    from {{ ref('int_price_technicals_base') }}

    {% if is_incremental() %}
        where dt_session_date :: timestamp > (select coalesce(max(dt_session_date), '1970-01-01' :: timestamp) from {{ this }})
    {% endif %}

),

sma as (
    select 
        src.*,
        avg.mnt_simple_moving_average_20,
        avg.mnt_simple_moving_average_50,
        avg.mnt_simple_moving_average_200
    from source src
    left join {{ ref('int_simple_moving_averages') }} avg
        using (desc_ticker, dt_session_date)

),

macd as (
    select 
        sma.*,
        mac.rt_moving_average_convergence_divergence_line,
        mac.rt_moving_average_convergence_divergence_signal,
        mac.rt_moving_average_convergence_divergence_histogram
    from sma sma
    left join {{ ref('int_moving_average_convergence_divergence') }} mac
        using (desc_ticker, dt_session_date)

),

bollinger as (
    select 
        mac.*,
        bol.mnt_bollinger_lower_band_20,
        bol.mnt_bollinger_mid_band_20,
        bol.mnt_bollinger_upper_band_20,
        bol.mnt_bollinger_zscore
    from macd mac
    left join {{ ref('int_bollinger_bands') }} bol
        using (desc_ticker, dt_session_date)

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        dt_session_date,
        rt_average_true_range, 
        pct_volatility_week, 
        pct_volatility_month, 
        mnt_simple_moving_average_20,
        pct_20_day_simple_moving_average_distance, 
        mnt_simple_moving_average_50,
        pct_50_day_simple_moving_average_distance, 
        mnt_simple_moving_average_200,
        pct_200_day_simple_moving_average_distance, 
        rt_relative_strength_index_14,
        rt_moving_average_convergence_divergence_line,
        rt_moving_average_convergence_divergence_signal,
        rt_moving_average_convergence_divergence_histogram,
        mnt_bollinger_lower_band_20,
        mnt_bollinger_mid_band_20,
        mnt_bollinger_upper_band_20,
        mnt_bollinger_zscore,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from bollinger        
)

select * from final