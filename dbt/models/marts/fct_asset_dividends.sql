with source as (

    select
        desc_ticker,
        mnt_annual_dividend_per_share,
        mnt_close_price,
        mnt_dividend_per_share_trail_twelve_months,
        dt_dividend_excluding_date,
        pct_dividend_yield,
        pct_payout_ratio,
        pct_dividend_growth_1_year,
        pct_dividend_growth_3_years,
        pct_dividend_growth_5_years,
        audit_loaded_at

    from {{ ref('stg_finviz__asset_attributes') }}
    where audit_loaded_at = (select coalesce(max(audit_loaded_at), 19700101) from {{ ref('stg_finviz__asset_attributes') }})
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        coalesce(mnt_annual_dividend_per_share, 0) as mnt_annual_dividend_per_share,
        coalesce(round(mnt_annual_dividend_per_share + mnt_close_price, 2), 0) :: double as mnt_total_return_per_share,
        coalesce(mnt_dividend_per_share_trail_twelve_months, 0) as mnt_dividend_per_share_trail_twelve_months,
        coalesce(dt_dividend_excluding_date, '1900-01-01') as dt_dividend_excluding_date,
        coalesce(pct_dividend_yield, 0) as pct_dividend_yield,
        coalesce(pct_payout_ratio, 0) as pct_payout_ratio,
        coalesce(pct_dividend_growth_1_year, 0) as pct_dividend_growth_1_year,
        coalesce(pct_dividend_growth_3_years, 0) as pct_dividend_growth_3_years,
        coalesce(pct_dividend_growth_5_years, 0) as pct_dividend_growth_5_years,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source
)

select * from final