{{ config(
    materialized='incremental',
    unique_key=['desc_ticker', 'audit_loaded_at']
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
        rt_relative_volume_ratio,
        audit_loaded_at
    
    from {{ ref('stg_finviz__asset_attributes') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

normalized as (
    select
        *,
        coalesce(percent_rank() over (order by pct_short_interest_rate), 0) as rt_normalized_short_interest_rate,
        coalesce(percent_rank() over (order by rt_short_ratio), 0) as rt_normalized_short_ratio,
        coalesce(percent_rank() over (order by -qty_float_shares), 0) as rt_normalized_float_shares,
        coalesce(percent_rank() over (order by rt_relative_volume_ratio), 0) as rt_normalized_relative_volume_ratio

    from source
),

gated as (
    select
        *,

        case
            when pct_short_interest_rate < 0.10 then 0.40
            when pct_short_interest_rate < 0.20 then 0.70
            else 1.00
        end as rt_gate_short_interest_rate,

        case
            when rt_relative_volume_ratio < 1.0 then 0.70
            when rt_relative_volume_ratio < 1.5 then 0.90
            else 1.00
        end as rt_gate_relative_volume_ratio

    from normalized
)

select * from gated