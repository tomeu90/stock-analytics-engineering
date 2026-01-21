{{ config(
    materialized='view'
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
),

indicators as (
    select
        src.*,
        mac.rt_moving_average_convergence_divergence_histogram,
        bol.mnt_bollinger_zscore,
        avg.mnt_simple_moving_average_20,
        avg.mnt_simple_moving_average_50,
        avg.mnt_simple_moving_average_200

    from source src
    left join {{ ref('int_moving_average_convergence_divergence') }} mac
        using (desc_ticker, dt_session_date)
    left join {{ ref('int_bollinger_bands') }} bol
        using (desc_ticker, dt_session_date)
    left join {{ ref('int_simple_moving_averages') }} avg
        using (desc_ticker, dt_session_date)
),

regime as (
    select
        *,
        case when mnt_simple_moving_average_20 > mnt_simple_moving_average_50 then 1 else 0 end as flag_20_over_50,
        case when mnt_simple_moving_average_50 > mnt_simple_moving_average_200 then 1 else 0 end as flag_50_over_200,
        (
            0.5 * case when mnt_simple_moving_average_20 > mnt_simple_moving_average_50 then 1 else 0 end +
            0.5 * case when mnt_simple_moving_average_50 > mnt_simple_moving_average_200 then 1 else 0 end
        ) as trend_regime_score

    from indicators
),

capped as (

    select
        *,
        case
            when pct_20_day_simple_moving_average_distance <= 0     then 0
            when pct_20_day_simple_moving_average_distance >= 0.08  then 0.08
            else pct_20_day_simple_moving_average_distance
        end as capped_dist_20,
        case
            when pct_50_day_simple_moving_average_distance <= 0     then 0
            when pct_50_day_simple_moving_average_distance >= 0.12  then 0.12
            else pct_50_day_simple_moving_average_distance
        end as capped_dist_50,
        case
            when pct_volatility_week <= 0.02 then 0.02
            when pct_volatility_week >= 0.08 then 0.08
            else pct_volatility_week
        end as capped_vol_week,
        case
            when pct_volatility_month <= 0.05 then 0.05
            when pct_volatility_month >= 0.20 then 0.20
            else pct_volatility_month
        end as capped_vol_month,
        case
            when mnt_bollinger_zscore < 0.5 then 0
            when mnt_bollinger_zscore > 2.5 then 0
            else 1 - abs(mnt_bollinger_zscore - 1.5) / 1.0
        end as shaped_bollinger,
        case
            when rt_relative_strength_index_14 < 40 then 0
            when rt_relative_strength_index_14 > 80 then 0
            else 1 - abs(rt_relative_strength_index_14 - 60) / 20
        end as shaped_rsi 

    from regime
),

trend as (

    select
        *,
        (0.6 * trend_regime_score + 0.4 * capped_dist_20) :: double as trend_score_20,
        (0.6 * trend_regime_score + 0.4 * capped_dist_50) :: double as trend_score_50

    from capped
),

normalized as (
    select
        *,
        coalesce(percent_rank() over (order by trend_score_20), 0) as rt_normalized_20_day_simple_moving_average_distance,
        coalesce(percent_rank() over (order by trend_score_50), 0) as rt_normalized_50_day_simple_moving_average_distance,
        coalesce(percent_rank() over (order by shaped_rsi), 0) as rt_normalized_relative_strength_index_14,
        coalesce(percent_rank() over (order by rt_moving_average_convergence_divergence_histogram), 0) as rt_normalized_moving_average_convergence_divergence_histogram,
        coalesce(percent_rank() over (order by rt_average_true_range), 0) as rt_normalized_average_true_range,
        coalesce(percent_rank() over (order by capped_vol_week), 0) as rt_normalized_volatility_week,
        coalesce(percent_rank() over (order by capped_vol_month), 0) as rt_normalized_volatility_month,
        coalesce(percent_rank() over (order by shaped_bollinger), 0) as rt_normalized_bollinger_zscore        

    from trend
)

select * from normalized