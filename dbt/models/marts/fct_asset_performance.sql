{% set columns = ['pct_performance_1_hour', 'pct_performance_2_hours', 'pct_performance_4_hours', 'pct_performance_day',
    'pct_performance_week', 'pct_performance_month', 'pct_performance_quarter', 'pct_50_day_high_distance',
    'pct_50_day_low_distance', 'pct_performance_half_year', 'pct_performance_year_to_date', 'pct_52_week_high_distance',
    'pct_52_week_low_distance', 'pct_performance_year', 'pct_performance_3_years', 'pct_performance_5_years',
    'pct_performance_10_years', 'pct_all_time_high_distance', 'pct_all_time_low_distance'] %}

with 

source as (

    select 
        desc_ticker,
        {% for col in columns %}
            coalesce({{ col }}, 0) as {{ col }},
        {% endfor %}
        audit_loaded_at
    
    from {{ ref('int_asset_performance_normalized') }}
    where audit_loaded_at = (select coalesce(max(audit_loaded_at), 19700101) from {{ ref('int_asset_performance_normalized') }})
),

metrics as (
    select
        *,
        case
            when pct_performance_day > 0
            and pct_performance_week > 0
            and pct_performance_month > 0
            and pct_performance_year > 0
        then true
        else false end :: boolean as flag_strong_uptrend,
        case
            when pct_performance_day < 0
            and pct_performance_week < 0
            and pct_performance_month < 0
            and pct_performance_year < 0
        then true
        else false end :: boolean as flag_strong_downtrend,
        round(pct_performance_day - pct_performance_week, 4) :: double as pct_momentum_acceleration_week,
        round(pct_performance_week - pct_performance_month, 4) :: double as pct_momentum_acceleration_month,
        round(pct_performance_month - pct_performance_year, 4) :: double as pct_momentum_acceleration_year,
        round(abs(pct_52_week_high_distance), 4) :: double as pct_drawdown_from_52_week_high,
        case
            when pct_52_week_low_distance between 0.1 and 0.4
            and pct_performance_week > 0
            and pct_performance_month > 0
        then true
        else false end :: boolean as flag_recovery_signal,
        round(
            (
            abs(pct_performance_1_hour) +
            abs(pct_performance_2_hours) +
            abs(pct_performance_4_hours)
            ) / 3,
        4) :: double as pct_average_intraday_move,   
        round(
            sqrt(
            (
            pow(pct_performance_day
                - (pct_performance_day + pct_performance_week + pct_performance_month) / 3, 2) +
            pow(pct_performance_week
                - (pct_performance_day + pct_performance_week + pct_performance_month) / 3, 2) +
            pow(pct_performance_month
                - (pct_performance_day + pct_performance_week + pct_performance_month) / 3, 2)
            ) / 3),
        4) :: double as pct_horizon_dispersion,
        round(abs(pct_52_week_high_distance) / nullif(abs(pct_52_week_low_distance), 0), 4) :: double as rt_upside_downside_ratio,
        round(pct_performance_month / nullif(abs(pct_52_week_high_distance), 0), 4) :: double as rt_momentum_to_drawdown_ratio,
        case
            when pct_all_time_high_distance > -0.05
            and pct_performance_month > 0.15
        then true
        else false end :: boolean as flag_overextension_signal

    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,        
        pct_performance_1_hour,
        pct_performance_2_hours,
        pct_performance_4_hours,
        pct_performance_day,
        pct_performance_week,
        pct_performance_month,
        pct_performance_quarter,
        pct_performance_half_year,
        pct_performance_year_to_date,
        pct_performance_year,
        pct_performance_3_years,
        pct_performance_5_years,
        pct_performance_10_years,
        pct_50_day_high_distance,
        pct_50_day_low_distance,
        pct_52_week_high_distance,
        pct_52_week_low_distance,
        pct_all_time_high_distance,
        pct_all_time_low_distance,
        flag_overextension_signal,
        rt_upside_downside_ratio,
        pct_drawdown_from_52_week_high,
        case
            when pct_52_week_high_distance > -0.1 then 'Near 52 week high'
            when pct_52_week_high_distance > -0.3 then 'Moderate drawdown from 52 week high'
        else 'Deep drawdown from 52 week high' end as desc_drawdown_from_52_week_high,
        rt_momentum_to_drawdown_ratio,
        flag_strong_uptrend,
        flag_strong_downtrend,
        pct_momentum_acceleration_week,
        case
            when pct_performance_day - pct_performance_week > 0.02 then 'Accelerating trend'
            when pct_performance_day - pct_performance_week < -0.02 then 'Decelerating trend'
        else 'Stable trend' end as desc_pct_momentum_acceleration_week_change,
        pct_momentum_acceleration_month,
        case
            when pct_performance_week - pct_performance_month > 0.02 then 'Accelerating trend'
            when pct_performance_week - pct_performance_month < -0.02 then 'Decelerating trend'
        else 'Stable trend' end as desc_pct_momentum_acceleration_month_change,
        pct_momentum_acceleration_year,
        case
            when pct_performance_month - pct_performance_year > 0.02 then 'Accelerating trend'
            when pct_performance_month - pct_performance_year < -0.02 then 'Decelerating trend'
        else 'Stable trend' end as desc_pct_momentum_acceleration_year_change,
        flag_recovery_signal,
        pct_average_intraday_move,
        pct_horizon_dispersion,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at
    
    from metrics
)

select * from final