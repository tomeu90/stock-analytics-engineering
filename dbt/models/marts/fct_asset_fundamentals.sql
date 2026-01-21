{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        pct_return_on_assets,
        pct_return_on_equity,
        pct_return_on_invested_capital,
        rt_current_ratio,
        rt_quick_ratio,
        rt_long_term_debt_equity_ratio,
        rt_total_debt_equity_ratio,
        pct_gross_margin,
        pct_operating_margin,
        pct_profit_margin,
        mnt_cash_per_share,
        qty_employees,
        mnt_company_income,
        mnt_company_sales,
        rt_earnings_per_share_trail_twelve_months,
        rt_earnings_per_share_next_quarter,
        dt_earnings_release_date,
        pct_earnings_per_share_surprise,
        pct_revenue_surprise,
        dt_initial_public_offer_date,
        pct_earnings_per_share_growth_past_5_years,
        pct_earnings_per_share_growth_past_3_years,
        pct_earnings_per_share_growth_quarter_over_quarter,
        pct_earnings_per_share_year_over_year_trail_twelve_months,
        pct_earnings_per_share_growth_this_year,
        pct_earnings_per_share_growth_next_year,
        pct_earnings_per_share_growth_next_5_years,
        pct_sales_growth_past_5_years,
        pct_sales_growth_past_3_years,
        pct_sales_growth_quarter_over_quarter, 
        pct_sales_year_over_year_trail_twelve_months,
        audit_loaded_at
    
    from {{ ref('int_asset_fundamentals_normalized') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

binned as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        pct_return_on_assets,
        case
            when pct_return_on_assets < 0 then 'Unprofitable'
            when pct_return_on_assets < 0.5 then 'Low Return on Assets'
            when pct_return_on_assets < 0.10 then 'Healthy Return on Assets'
            when pct_return_on_assets < 0.20 then 'Strong Return on Assets'
            else 'Exceptional Return on Assets'
        end as desc_return_on_assets,
        pct_return_on_equity,
        case
            when pct_return_on_equity < 0 then 'Negative Return on Equity'
            when pct_return_on_equity < 0.10 then 'Low Return on Equity'
            when pct_return_on_equity < 0.20 then 'Healthy Return on Equity'
            when pct_return_on_equity < 0.35 then 'Strong Return on Equity'
            else 'Very High Return on Equity'
        end as desc_return_on_equity,
        pct_return_on_invested_capital,
        case
            when pct_return_on_invested_capital < 0.05 then 'Weak Return on Invested Capital'
            when pct_return_on_invested_capital < 0.10 then 'Adequate Return on Invested Capital'
            when pct_return_on_invested_capital < 0.20 then 'Strong Return on Invested Capital'
            else 'Excellent Return on Invested Capital'
        end as desc_return_on_invested_capital,
        rt_current_ratio,
        case
            when rt_current_ratio < 1 then 'Liquidity risk'
            when rt_current_ratio < 1.5 then 'Thin liquidity'
            when rt_current_ratio < 3 then 'Healthy liquidity'
            else 'Excess liquidity'
        end as desc_current_ratio,
        rt_quick_ratio,
        case
            when rt_quick_ratio < 0.8 then 'Liquidity stress'
            when rt_quick_ratio < 1.2 then 'Adequate liquidity'
            when rt_quick_ratio < 2.0 then 'Strong liquidity'
            else 'Very strong liquidity'
        end as desc_quick_ratio,
        rt_long_term_debt_equity_ratio,
        rt_total_debt_equity_ratio,
        case
            when rt_total_debt_equity_ratio < 0.3 then 'Low leverage'
            when rt_total_debt_equity_ratio < 1.0 then 'Moderate leverage'
            when rt_total_debt_equity_ratio < 2.0 then 'High leverage'
            else 'Very high Leverage'
        end as desc_total_debt_equity_ratio,
        pct_gross_margin,
        case
            when pct_gross_margin < 0.20 then 'Low margin'
            when pct_gross_margin < 0.40 then 'Average margin'
            when pct_gross_margin < 0.60 then 'High margin'
            else 'Exceptional margin'
        end as desc_gross_margin,
        pct_operating_margin,
        case
            when pct_operating_margin < 0 then 'Operating loss'
            when pct_operating_margin < 0.10 then 'Thin margin'
            when pct_operating_margin < 0.25 then 'Healthy margin'
            else 'Strong margin'
        end as desc_operating_margin,
        pct_profit_margin,
        case
            when pct_profit_margin < 0 then 'Unprofitable'
            when pct_profit_margin < 0.10 then 'Low profitability'
            when pct_profit_margin < 0.25 then 'Strong profitability'
            else 'Very profitable'
        end as desc_profit_margin,
        mnt_cash_per_share,
        qty_employees,
        mnt_company_income,
        mnt_company_sales,
        rt_earnings_per_share_trail_twelve_months,
        rt_earnings_per_share_next_quarter,
        dt_earnings_release_date,
        pct_earnings_per_share_surprise,
        case
            when pct_earnings_per_share_surprise < -0.10 then 'Large miss'
            when pct_earnings_per_share_surprise < 0 then 'Miss'
            when pct_earnings_per_share_surprise < 0.10 then 'Inline'
            else 'Beat'
        end as desc_earnings_per_share_surprise,
        pct_revenue_surprise,
        case
            when pct_revenue_surprise < -0.05 then 'Revenue miss'
            when pct_revenue_surprise < 0 then 'Slight miss'
            when pct_revenue_surprise < 0.05 then 'Inline'
            else 'Revenue beat'
        end as desc_revenue_surprise,
        dt_initial_public_offer_date,
        case
            when datediff(year, dt_initial_public_offer_date, to_date(cast(audit_loaded_at as string), 'yyyyMMdd')) < 3
                then 'Recent IPO'
            when datediff(year, dt_initial_public_offer_date, to_date(cast(audit_loaded_at as string), 'yyyyMMdd')) < 10
                then 'Mid-Life Company'
            else 'Mature Company'
        end as desc_company_maturity,
        pct_earnings_per_share_growth_past_5_years,
        pct_earnings_per_share_growth_past_3_years,
        pct_earnings_per_share_growth_quarter_over_quarter,
        pct_earnings_per_share_year_over_year_trail_twelve_months,
        pct_earnings_per_share_growth_this_year,
        pct_earnings_per_share_growth_next_year,
        pct_earnings_per_share_growth_next_5_years,
        pct_sales_growth_past_5_years,
        pct_sales_growth_past_3_years,
        pct_sales_growth_quarter_over_quarter, 
        pct_sales_year_over_year_trail_twelve_months,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source
)

select * from binned