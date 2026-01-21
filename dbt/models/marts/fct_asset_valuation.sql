{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        desc_industry,
        mnt_close_price,
        mnt_market_capitalization,
        rt_price_earnings_ratio, 
        rt_forward_price_earnings_ratio,
        rt_price_earnings_growth_ratio, 
        rt_price_sales_ratio, 
        rt_price_book_ratio, 
        rt_price_cash_ratio,
        rt_price_free_cash_flow_ratio, 
        rt_enterprise_value_ebitda_ratio, 
        rt_enterprise_value_sales_ratio, 
        cod_analyst_recommendation,
        mnt_target_price,
        audit_loaded_at

    from {{ ref('int_asset_valuation_normalized') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

hashed as (    
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        mnt_market_capitalization,
        rt_price_earnings_ratio, 
        rt_forward_price_earnings_ratio,
        rt_price_earnings_growth_ratio, 
        rt_price_sales_ratio, 
        rt_price_book_ratio, 
        rt_price_cash_ratio,
        rt_price_free_cash_flow_ratio, 
        rt_enterprise_value_ebitda_ratio, 
        rt_enterprise_value_sales_ratio, 
        cod_analyst_recommendation,
        case 
            when round(cod_analyst_recommendation) = 1 then 'Strong Buy'
            when round(cod_analyst_recommendation) = 2 then 'Buy'
            when round(cod_analyst_recommendation) = 3 then 'Hold'
            when round(cod_analyst_recommendation) = 4 then 'Sell'
            when round(cod_analyst_recommendation) = 5 then 'Strong Sell'
        else 'Not Covered' end as desc_analyst_recommendation,        
        mnt_target_price,
        case 
            when mnt_target_price <= mnt_close_price then 0 
            when mnt_target_price > mnt_close_price then 1 
        else null end as flag_price_below_target,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at        

    from source
)

select * from hashed