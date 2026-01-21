{{ config(
    materialized='incremental',
    unique_key=['desc_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        pct_performance_1_hour,
        pct_performance_2_hours,
        pct_performance_4_hours,
        pct_price_change as pct_performance_day,
        pct_performance_week,
        pct_performance_month,
        pct_performance_quarter,
        pct_50_day_high_distance,
        pct_50_day_low_distance,
        pct_performance_half_year,
        pct_performance_year_to_date,
        pct_52_week_high_distance,
        pct_52_week_low_distance,
        pct_performance_year,
        pct_performance_3_years,
        pct_performance_5_years,
        pct_performance_10_years,
        pct_all_time_high_distance,
        pct_all_time_low_distance,
        audit_loaded_at
    
    from {{ ref('stg_finviz__asset_attributes') }}
    
    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

momentum as (
    select
        *,
        (
            0.4 * pct_performance_month +
            0.3 * pct_performance_half_year +
            0.3 * pct_performance_year
        ) as rt_normalized_raw_momentum,
        (
            (pct_performance_month > 0) :: integer +
            (pct_performance_half_year > 0) :: integer +
            (pct_performance_year > 0) :: integer
        ) / 3.0 as rt_normalized_momentum_consistency,
        greatest(pct_52_week_high_distance, -0.5) as rt_normalized_capped_drawdown

    from source
),

scored as (
    select
         *,
         rt_normalized_raw_momentum * rt_normalized_momentum_consistency * (1 + rt_normalized_capped_drawdown) as rt_normalized_momentum_score_raw
    from momentum
),

normalized as (
    select
        *,
        coalesce(percent_rank() over (order by rt_normalized_momentum_score_raw), 0) as rt_normalized_momentum_score
    from scored
)

select * from normalized