{{ config(
    materialized='incremental',
    unique_key=['desc_ticker', 'audit_loaded_at']
) }}

with source as (

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
        pct_earnings_per_share_year_over_year_trail_twelve_months,
        pct_earnings_per_share_growth_past_5_years,
        pct_earnings_per_share_growth_past_3_years,
        pct_earnings_per_share_growth_quarter_over_quarter,
        pct_earnings_per_share_growth_this_year,
        pct_earnings_per_share_growth_next_year,
        pct_earnings_per_share_growth_next_5_years,
        pct_sales_growth_past_3_years,
        pct_sales_growth_past_5_years,
        pct_sales_growth_quarter_over_quarter, 
        pct_sales_year_over_year_trail_twelve_months,
        audit_loaded_at

    from {{ ref('stg_finviz__asset_attributes') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

shaped as (
    select
        *,
        case
            when rt_current_ratio < 1.0 then 0
            when rt_current_ratio > 3.0 then 1
            else (rt_current_ratio - 1.0) / 2.0
        end as rt_shaped_current_ratio,
        case
            when rt_quick_ratio < 0.8 then 0
            when rt_quick_ratio > 2.0 then 1
            else (rt_quick_ratio - 0.8) / 1.2
        end as rt_shaped_quick_ratio,
        case
            when rt_total_debt_equity_ratio <= 0 then 1
            when rt_total_debt_equity_ratio >= 2.0 then 0
            else 1 - (rt_total_debt_equity_ratio / 2.0)
        end as rt_shaped_total_debt_equity_ratio

    from source
),

normalized as (
    select
        *,
        coalesce(percent_rank() over (order by pct_gross_margin), 0) as rt_normalized_gross_margin,
        coalesce(percent_rank() over (order by pct_operating_margin), 0) as rt_normalized_operating_margin,
        coalesce(percent_rank() over (order by pct_profit_margin), 0) as rt_normalized_profit_margin,
        coalesce(percent_rank() over (order by pct_return_on_assets), 0) as rt_normalized_return_on_assets,
        coalesce(percent_rank() over (order by pct_return_on_equity), 0) as rt_normalized_return_on_equity,
        coalesce(percent_rank() over (order by pct_return_on_invested_capital), 0) as rt_normalized_return_on_invested_capital,
        coalesce(percent_rank() over (order by rt_shaped_current_ratio), 0) as rt_normalized_current_ratio,
        coalesce(percent_rank() over (order by rt_shaped_quick_ratio), 0) as rt_normalized_quick_ratio,
        coalesce(percent_rank() over (order by rt_shaped_total_debt_equity_ratio), 0) as rt_normalized_total_debt_equity_ratio,
        coalesce(percent_rank() over (order by pct_earnings_per_share_year_over_year_trail_twelve_months), 0) as 
        rt_normalized_earnings_per_share_year_over_year_trail_twelve_months,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_past_5_years), 0) as rt_normalized_earnings_per_share_growth_past_5_years,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_past_3_years), 0) as rt_normalized_earnings_per_share_growth_past_3_years,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_quarter_over_quarter), 0) as rt_normalized_earnings_per_share_growth_quarter_over_quarter,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_this_year), 0) as rt_normalized_earnings_per_share_growth_this_year,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_next_year), 0) as rt_normalized_earnings_per_share_growth_next_year,
        coalesce(percent_rank() over (order by pct_earnings_per_share_growth_next_5_years), 0) as rt_normalized_earnings_per_share_growth_next_5_years,
        coalesce(percent_rank() over (order by pct_sales_growth_past_3_years), 0) as rt_normalized_sales_growth_past_3_years,
        coalesce(percent_rank() over (order by pct_sales_growth_past_5_years), 0) as rt_normalized_sales_growth_past_5_years,
        coalesce(percent_rank() over (order by pct_sales_growth_quarter_over_quarter), 0) as rt_normalized_sales_growth_quarter_over_quarter,
        coalesce(percent_rank() over (order by pct_sales_year_over_year_trail_twelve_months), 0) as rt_normalized_sales_year_over_year_trail_twelve_months,
        coalesce(percent_rank() over (order by pct_earnings_per_share_surprise), 0) as rt_normalized_earnings_per_share_surprise,
        coalesce(percent_rank() over (order by pct_revenue_surprise), 0) as rt_normalized_revenue_surprise

    from shaped
),

grouped as (
    select
        *,
        (
            0.30 * rt_normalized_earnings_per_share_growth_quarter_over_quarter +
            0.30 * rt_normalized_earnings_per_share_year_over_year_trail_twelve_months +
            0.20 * rt_normalized_sales_growth_quarter_over_quarter +
            0.20 * rt_normalized_sales_year_over_year_trail_twelve_months
        ) as rt_normalized_growth_near_term,
        (
            0.40 * rt_normalized_earnings_per_share_growth_this_year +
            0.40 * rt_normalized_earnings_per_share_growth_next_year +
            0.20 * rt_normalized_earnings_per_share_growth_next_5_years
        ) as rt_normalized_growth_forward,
        (
            0.30 * rt_normalized_earnings_per_share_growth_past_3_years +
            0.20 * rt_normalized_earnings_per_share_growth_past_5_years +
            0.25 * rt_normalized_sales_growth_past_3_years +
            0.25 * rt_normalized_sales_growth_past_5_years
        ) as rt_normalized_growth_structural
    
    from normalized
),

scored as (
    select
        *,
        (
            0.40 * rt_normalized_growth_near_term +
            0.35 * rt_normalized_growth_forward +
            0.25 * rt_normalized_growth_structural
        ) as rt_normalized_fundamental_growth_factor 

    from grouped       
)

select * from scored