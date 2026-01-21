{{ config(
    materialized='incremental',
    unique_key=['sk_asset_ticker', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_ticker,
        qty_outstanding_shares,
        qty_float_shares,
        pct_float_shares,
        pct_insider_ownership,
        pct_insider_transactions,
        pct_institutional_ownership,
        pct_institutional_transactions,
        flag_shortable_asset,
        pct_short_interest_rate,
        pct_short_float_shares,
        rt_short_ratio,
        flag_optionable_asset,
        audit_loaded_at
    
    from {{ ref('int_asset_ownership_normalized') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        qty_outstanding_shares,
        qty_float_shares,
        case
            when qty_float_shares < 10000000 then 'Micro float'
            when qty_float_shares < 50000000 then 'Small float'
            when qty_float_shares < 200000000 then 'Medium float'
            else 'Large float'
        end as desc_float_shares_quantity,
        pct_float_shares,
        case
            when pct_float_shares < 0.25 then 'Very tight float'
            when pct_float_shares < 0.50 then 'Tight float'
            when pct_float_shares < 0.75 then 'Normal float'
            else 'Wide float'
        end as desc_float_shares_percentage,
        pct_insider_ownership,
        case
            when pct_insider_ownership < 0.01 then 'Minimal insider ownership'
            when pct_insider_ownership < 0.05 then 'Low insider ownership'
            when pct_insider_ownership < 0.10 then 'Moderate insider ownership'
            else 'High insider ownership'
        end as desc_insider_ownership,
        pct_insider_transactions,
        case
            when pct_insider_transactions < 0 then 'Net selling'
            when pct_insider_transactions = 0 then 'Neutral'
            else 'Net buying'
        end as desc_insider_transactions,
        pct_institutional_ownership,
        case
            when pct_institutional_ownership < 0.25 then 'Low institutional presence'
            when pct_institutional_ownership < 0.50 then 'Moderate institutional presence'
            when pct_institutional_ownership < 0.75 then 'High institutional presence'
            else 'Very high institutional presence'
        end as desc_institutional_ownership,
        pct_institutional_transactions,
        case
            when pct_institutional_transactions < 0 then 'Net selling'
            when pct_institutional_transactions = 0 then 'Neutral'
            else 'Net buying'
        end as desc_institutional_transactions,
        flag_shortable_asset,
        pct_short_interest_rate,
        case
            when pct_short_interest_rate < 0.05 then 'Low short interest'
            when pct_short_interest_rate < 0.10 then 'Moderate short interest'
            when pct_short_interest_rate < 0.20 then 'High short interest'
            else 'Extreme short interest'
        end as desc_short_interest_rate,
        pct_short_float_shares,
        case
            when pct_short_float_shares < 0.05 then 'Low short float'
            when pct_short_float_shares < 0.10 then 'Moderate short float'
            when pct_short_float_shares < 0.20 then 'High short float'
            else 'Crowded short'
        end as desc_short_float_shares,
        rt_short_ratio,
        case
            when rt_short_ratio < 1 then 'Very easy to cover'
            when rt_short_ratio < 3 then 'Easy to cover'
            when rt_short_ratio < 7 then 'Hard to cover'
            else 'Very hard to cover'
        end as desc_short_ratio,
        flag_optionable_asset,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source        
)

select * from hashed