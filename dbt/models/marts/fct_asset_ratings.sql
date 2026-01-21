{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        rt_normalized_market_capitalization,
        rt_normalized_price_earnings_ratio, 
        rt_normalized_forward_price_earnings_ratio,
        rt_normalized_price_earnings_growth_ratio, 
        rt_normalized_price_sales_ratio, 
        rt_normalized_price_book_ratio, 
        rt_normalized_price_cash_ratio,
        rt_normalized_price_free_cash_flow_ratio, 
        rt_normalized_enterprise_value_ebitda_ratio, 
        rt_normalized_enterprise_value_sales_ratio, 
        rt_normalized_analyst_recommendation,
        audit_loaded_at

    from {{ ref('int_asset_valuation_normalized') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

technicals as (
    select
        src.*,
        tec.trend_regime_score,
        tec.rt_normalized_20_day_simple_moving_average_distance,
        tec.rt_normalized_50_day_simple_moving_average_distance,
        tec.rt_normalized_relative_strength_index_14,
        tec.rt_normalized_moving_average_convergence_divergence_histogram,
        tec.rt_normalized_average_true_range,
        tec.rt_normalized_volatility_week,
        tec.rt_normalized_volatility_month,
        tec.rt_normalized_bollinger_zscore
    from source src
    left join {{ ref('int_asset_technicals_normalized') }} tec
        on src.audit_loaded_at = tec.audit_loaded_at
        and src.desc_ticker = tec.desc_ticker

    {% if is_incremental() %}
        and tec.audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

shorts as (
    select
        tec.*,
        shr.rt_normalized_short_interest_rate,
        shr.rt_normalized_short_ratio,
        shr.rt_normalized_float_shares,
        shr.rt_normalized_relative_volume_ratio,
        shr.rt_gate_short_interest_rate,
        shr.rt_gate_relative_volume_ratio

    from technicals tec
    left join {{ ref('int_asset_ownership_normalized') }} shr
        on flag_shortable_asset = true
        and shr.audit_loaded_at = tec.audit_loaded_at
        and shr.desc_ticker = tec.desc_ticker

    {% if is_incremental() %}
        and shr.audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

fundamentals as (
    select
        shr.*,
        fun.rt_normalized_gross_margin,
        fun.rt_normalized_operating_margin,
        fun.rt_normalized_profit_margin,
        fun.rt_normalized_return_on_assets,
        fun.rt_normalized_return_on_equity,
        fun.rt_normalized_return_on_invested_capital,
        fun.rt_normalized_current_ratio,
        fun.rt_normalized_quick_ratio,
        fun.rt_normalized_total_debt_equity_ratio,
        fun.rt_normalized_fundamental_growth_factor,
        fun.rt_normalized_earnings_per_share_surprise,
        fun.rt_normalized_revenue_surprise

    from shorts shr
    left join {{ ref('int_asset_fundamentals_normalized') }} fun
        on shr.audit_loaded_at = fun.audit_loaded_at
        and shr.desc_ticker = fun.desc_ticker

    {% if is_incremental() %}
        and fun.audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

performance as (
    select
        fun.*,
        per.rt_normalized_momentum_score

    from fundamentals fun
    left join {{ ref('int_asset_performance_normalized') }} per
        on per.audit_loaded_at = fun.audit_loaded_at
        and per.desc_ticker = fun.desc_ticker

    {% if is_incremental() %}
        and per.audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

news as (
    select
        per.*,
        new.rt_compound_sentiment_score,
        new.rt_sentiment_score_certainty

    from performance per
    left join {{ ref('int_news_scores_exploded') }} new
        on per.audit_loaded_at = new.audit_loaded_at
        and per.desc_ticker = new.desc_news_related_tickers

    {% if is_incremental() %}
        and new.audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        round(
            (
                (0.15 * rt_normalized_market_capitalization) +
                (0.15 * rt_normalized_price_earnings_ratio) +
                (0.1  * rt_normalized_forward_price_earnings_ratio) +
                (0.1  * rt_normalized_price_earnings_growth_ratio) +
                (0.1  * rt_normalized_enterprise_value_ebitda_ratio) +
                (0.1  * rt_normalized_analyst_recommendation) +
                (0.1  * rt_normalized_price_free_cash_flow_ratio) +
                (0.2  * rt_normalized_price_cash_ratio)
            ),
            4
        ) :: double as rt_value_strength_score,
       round(
            (
                (0.25 * trend_regime_score) +
                (0.12 * rt_normalized_20_day_simple_moving_average_distance) +
                (0.08 * rt_normalized_50_day_simple_moving_average_distance) +
                (0.18 * rt_normalized_relative_strength_index_14) +
                (0.12 * rt_normalized_moving_average_convergence_divergence_histogram) +
                (0.10 * rt_normalized_average_true_range) +
                (0.05 * rt_normalized_volatility_week) +
                (0.05 * rt_normalized_volatility_month) +
                (0.05 * rt_normalized_bollinger_zscore)
            ),
            4
        ) :: double as rt_technical_strength_score,
        round(
            (
                (0.30 * rt_normalized_short_interest_rate) +
                (0.25 * rt_normalized_short_ratio) +
                (0.20 * rt_normalized_float_shares) +
                (0.25 * rt_normalized_relative_volume_ratio)
            ),
            4
        ) :: double as rt_short_squeeze_score,
        round(
            (
                (0.12 * rt_normalized_gross_margin) +
                (0.12 * rt_normalized_operating_margin) +
                (0.11 * rt_normalized_profit_margin) +
                (0.07 * rt_normalized_return_on_assets) +
                (0.07 * rt_normalized_return_on_equity) +
                (0.06 * rt_normalized_return_on_invested_capital) +
                (0.07 * rt_normalized_current_ratio) +
                (0.06 * rt_normalized_quick_ratio) +
                (0.07 * rt_normalized_total_debt_equity_ratio) +
                (0.17 * rt_normalized_fundamental_growth_factor) +    
                (0.04 * rt_normalized_earnings_per_share_surprise) +
                (0.04 * rt_normalized_revenue_surprise)
            ),
            4
        ) :: double as rt_fundamental_strength_score,
        round(rt_normalized_momentum_score, 4) :: double as rt_momentum_strength_score,
        coalesce(round((rt_compound_sentiment_score * rt_sentiment_score_certainty), 4), 0) :: double as rt_sentiment_strength_score,      
        audit_loaded_at

    from news        
),

scores as (
    select
        sk_asset_ticker,
        rt_value_strength_score,
        rt_technical_strength_score,
        coalesce(rt_short_squeeze_score, 0) :: double as rt_short_squeeze_score,
        coalesce(round((0.7 * rt_short_squeeze_score) + (0.3 * rt_technical_strength_score), 4), 0) :: double as rt_short_squeeze_momentum_score,
        rt_fundamental_strength_score,
        rt_momentum_strength_score,
        rt_sentiment_strength_score,
        coalesce(round(
            ({{ var("value_strength_score", "0.5") }} * rt_value_strength_score) + 
            ({{ var("technical_strength_score", "0.3") }} * rt_technical_strength_score) +
            ({{ var("fundamental_strength_score", "0.08") }} * rt_fundamental_strength_score) +
            ({{ var("momentum_strength_score", "0.1") }} * rt_momentum_strength_score) +
            ({{ var("sentiment_strength_score", "0.02") }} * rt_sentiment_strength_score)
            , 4), 0) :: double as rt_overall_score,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from hashed
)

select * from scores