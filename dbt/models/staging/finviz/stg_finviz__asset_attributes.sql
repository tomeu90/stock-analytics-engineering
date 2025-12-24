with 

source as (

    select * from {{ source('finviz', 'screener_data') }}

),

renamed as (

    select

        -- asset identity
        ticker :: string as desc_ticker,
        company :: string as desc_company,
        sector :: string as desc_sector,
        industry :: string as desc_industry,
        country :: string as desc_country,
        exchange :: string as desc_exchange,
        index :: string as desc_index,

        -- asset prices
        prev_close :: decimal(10,2) as mnt_previous_close_price,
        round(regexp_replace(gap, '%', '') / 100 :: decimal, 4) as pct_price_gap,
        round(open, 2) :: decimal(10,2) as mnt_open_price,
        round(high, 2) :: decimal(10,2) as mnt_high_price,
        round(low, 2) :: decimal(10,2) as mnt_low_price,
        round(price, 2) :: decimal(10,2) as mnt_close_price,
        round(after_hours_close, 2) :: decimal(10,2) as mnt_after_hours_close_price,
        trades :: integer as qty_trades,
        volume :: integer as qty_volume,
        average_volume :: integer as qty_average_volume,
        relative_volume :: double as rt_relative_volume_ratio,
        after_hours_volume :: integer as qty_after_hours_volume,
        round(regexp_replace(change, '%', '') / 100 :: decimal, 4) as pct_price_change,
        round(regexp_replace(change_from_open, '%', '') / 100 :: decimal, 4) as pct_price_change_from_open,
        round(regexp_replace(after_hours_change, '%', '') / 100 :: decimal, 4) as pct_after_hours_price_change,
        try_cast(trim(split(52_week_range, '-')[0]) as decimal(10,2)) as mnt_52_week_low_price,
        try_cast(trim(split(52_week_range, '-')[1]) as decimal(10,2)) as mnt_52_week_high_price,

        -- asset valuation
        (market_cap * 1000000) :: double as mnt_market_capitalization,
        p_e :: double as rt_price_earnings_ratio,
        forward_p_e :: double as rt_forward_price_earnings_ratio,
        peg :: double as rt_price_earnings_growth_ratio,
        p_s :: double as rt_price_sales_ratio,
        p_b :: double as rt_price_book_ratio,
        p_cash :: double as rt_price_cash_ratio,
        p_free_cash_flow :: double as rt_price_free_cash_flow_ratio,
        book_sh :: double as rt_book_shares,
        (enterprise_value * 1000000) :: double as mnt_enterprise_value,
        ev_ebitda :: double as rt_enterprise_value_ebitda_ratio,
        ev_sales :: double as rt_enterprise_value_sales_ratio,
        analyst_recom :: double as cod_analyst_recommendation,
        round(target_price, 2) :: decimal(10, 2) as mnt_target_price,

        -- asset ownership
        (shares_outstanding * 1000000) :: double as qty_outstanding_shares,
        (shares_float * 1000000) :: double as qty_float_shares,
        round(regexp_replace(float_pct, '%', '') / 100 :: decimal, 4) as pct_float_shares,
        round(regexp_replace(insider_ownership, '%', '') / 100 :: decimal, 4) as pct_insider_ownership,
        round(regexp_replace(insider_transactions, '%', '') / 100 :: decimal, 4) as pct_insider_transactions,
        round(regexp_replace(institutional_ownership, '%', '') / 100 :: decimal, 4) as pct_institutional_ownership,
        round(regexp_replace(institutional_transactions, '%', '') / 100 :: decimal, 4) as pct_institutional_transactions,
        {{ string_to_boolean('shortable') }} :: boolean as flag_shortable_asset,
        round(short_interest / 100 :: decimal, 4) as pct_short_interest_rate,
        round(regexp_replace(short_float, '%', '') / 100 :: decimal, 4) as pct_short_float_shares,
        short_ratio :: double as rt_short_ratio,
        {{ string_to_boolean('optionable') }} :: boolean as flag_optionable_asset,

        -- asset dividends
        dividend :: double as mnt_annual_dividend_per_share,
        dividend_ttm :: double as mnt_dividend_per_share_trail_twelve_months,
        {{ parse_mixed_timestamps('dividend_ex_date') }} :: timestamp as dt_dividend_excluding_date,
        round(regexp_replace(dividend_yield, '%', '') / 100 :: decimal, 4) as pct_dividend_yield,
        round(regexp_replace(payout_ratio, '%', '') / 100 :: decimal, 4) as pct_payout_ratio,

        -- asset fundamentals
        round(regexp_replace(return_on_assets, '%', '') / 100 :: decimal, 4) as pct_return_on_assets,
        round(regexp_replace(return_on_equity, '%', '') / 100 :: decimal, 4) as pct_return_on_equity,
        round(regexp_replace(return_on_invested_capital, '%', '') / 100 :: decimal, 4) as pct_return_on_invested_capital,
        current_ratio :: double as rt_current_ratio,
        quick_ratio :: double as rt_quick_ratio,
        lt_debt_equity :: double as rt_long_term_debt_equity_ratio,
        total_debt_equity :: double  rt_total_debt_equity_ratio,
        round(regexp_replace(gross_margin, '%', '') / 100 :: decimal, 4) as pct_gross_margin,
        round(regexp_replace(operating_margin, '%', '') / 100 :: decimal, 4) as pct_operating_margin,
        round(regexp_replace(profit_margin, '%', '') / 100 :: decimal, 4) as pct_profit_margin,
        cash_sh :: double as mnt_cash_per_share,
        employees :: double as qty_employees,
        (income * 1000000) :: double as mnt_company_income,
        (sales * 1000000) :: double as mnt_company_sales,
        eps_ttm :: double as rt_earnings_per_share_trail_twelve_months,
        eps_next_q :: double as rt_earnings_per_share_next_quarter,
        {{ parse_mixed_timestamps('earnings_date') }} :: timestamp as dt_earnings_release_date,
        round(regexp_replace(eps_surprise, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_surprise,
        round(regexp_replace(revenue_surprise, '%', '') / 100 :: decimal, 4) as pct_revenue_surprise,
        {{ parse_mixed_timestamps('ipo_date') }} :: timestamp as dt_initial_public_offer_date,
        round(regexp_replace(eps_year_over_year_ttm, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_year_over_year_trail_twelve_months,
        round(regexp_replace(sales_year_over_year_ttm, '%', '') / 100 :: decimal, 4) as pct_sales_year_over_year_trail_twelve_months,

        -- asset technicals
        beta :: double as rt_beta_coefficient,
        average_true_range :: double as rt_average_true_range,
        round(regexp_replace(volatility_week, '%', '') / 100 :: decimal, 4) as pct_volatility_week,
        round(regexp_replace(volatility_month, '%', '') / 100 :: decimal, 4) as pct_volatility_month,
        round(regexp_replace(20_day_simple_moving_average, '%', '') / 100 :: decimal, 4) as pct_20_day_simple_moving_average_distance,
        round(regexp_replace(50_day_simple_moving_average, '%', '') / 100 :: decimal, 4) as pct_50_day_simple_moving_average_distance,
        round(regexp_replace(200_day_simple_moving_average, '%', '') / 100 :: decimal, 4) as pct_200_day_simple_moving_average_distance,
        relative_strength_index_14 :: double as rt_relative_strength_index_14,

        -- asset growth
        round(regexp_replace(eps_growth_past_5_years, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_past_5_years,
        round(regexp_replace(eps_growth_past_3_years, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_past_3_years,
        round(regexp_replace(eps_growth_quarter_over_quarter, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_quarter_over_quarter,
        round(regexp_replace(eps_growth_this_year, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_this_year,
        round(regexp_replace(eps_growth_next_year, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_next_year,
        round(regexp_replace(eps_growth_next_5_years, '%', '') / 100 :: decimal, 4) as pct_earnings_per_share_growth_next_5_years,
        round(regexp_replace(sales_growth_past_3_years, '%', '') / 100 :: decimal, 4) as pct_sales_growth_past_3_years,
        round(regexp_replace(sales_growth_past_5_years, '%', '') / 100 :: decimal, 4) as pct_sales_growth_past_5_years,
        round(regexp_replace(sales_growth_quarter_over_quarter, '%', '') / 100 :: decimal, 4) as pct_sales_growth_quarter_over_quarter,
        round(regexp_replace(dividend_growth_1_year, '%', '') / 100 :: decimal, 4) as pct_dividend_growth_1_year,
        round(regexp_replace(dividend_growth_3_years, '%', '') / 100 :: decimal, 4) as pct_dividend_growth_3_years,
        round(regexp_replace(dividend_growth_5_years, '%', '') / 100 :: decimal, 4) as pct_dividend_growth_5_years,

        -- asset performance
        round(regexp_replace(performance_1_hour, '%', '') / 100 :: decimal, 4) as pct_performance_1_hour,
        round(regexp_replace(performance_2_hours, '%', '') / 100 :: decimal, 4) as pct_performance_2_hours,
        round(regexp_replace(performance_4_hours, '%', '') / 100 :: decimal, 4) as pct_performance_4_hours,
        round(regexp_replace(performance_week, '%', '') / 100 :: decimal, 4) as pct_performance_week,
        round(regexp_replace(performance_month, '%', '') / 100 :: decimal, 4) as pct_performance_month,
        round(regexp_replace(performance_quarter, '%', '') / 100 :: decimal, 4) as pct_performance_quarter,
        round(regexp_replace(50_day_high, '%', '') / 100 :: decimal, 4) as pct_50_day_high_distance,
        round(regexp_replace(50_day_low, '%', '') / 100 :: decimal, 4) as pct_50_day_low_distance,
        round(regexp_replace(performance_half_year, '%', '') / 100 :: decimal, 4) as pct_performance_half_year,
        round(regexp_replace(performance_ytd, '%', '') / 100 :: decimal, 4) as pct_performance_year_to_date,
        round(regexp_replace(52_week_high, '%', '') / 100 :: decimal, 4) as pct_52_week_high_distance,
        round(regexp_replace(52_week_low, '%', '') / 100 :: decimal, 4) as pct_52_week_low_distance,
        round(regexp_replace(performance_year, '%', '') / 100 :: decimal, 4) as pct_performance_year,
        round(regexp_replace(performance_3_years, '%', '') / 100 :: decimal, 4) as pct_performance_3_years,
        round(regexp_replace(performance_5_years, '%', '') / 100 :: decimal, 4) as pct_performance_5_years,
        round(regexp_replace(performance_10_years, '%', '') / 100 :: decimal, 4) as pct_performance_10_years,
        round(regexp_replace(all_time_high, '%', '') / 100 :: decimal, 4) as pct_all_time_high_distance,
        round(regexp_replace(all_time_low, '%', '') / 100 :: decimal, 4) as pct_all_time_low_distance,

        -- asset news
        news_title :: string as desc_news_title,
        {{ parse_mixed_timestamps('news_time') }} :: timestamp as dt_news_posting,
        news_url :: string as desc_news_url,        

        -- audit attributes
        load_date :: integer as audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source

)

select * from renamed