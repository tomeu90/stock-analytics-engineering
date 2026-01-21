{{ config(
    materialized='incremental',
    unique_key=['desc_ticker', 'audit_loaded_at']
) }}

{% set metrics = ['rt_price_earnings_ratio', 'rt_forward_price_earnings_ratio', 'rt_price_earnings_growth_ratio', 
'rt_price_sales_ratio', 'rt_price_book_ratio', 'rt_price_cash_ratio', 'rt_price_free_cash_flow_ratio', 
'rt_enterprise_value_ebitda_ratio', 'rt_enterprise_value_sales_ratio', 'cod_analyst_recommendation'] %}

with 

source as (

    select 
        desc_ticker,
        desc_industry,
        mnt_close_price,
        mnt_target_price,
        mnt_market_capitalization,
        audit_loaded_at,

        {% for metric in metrics %}
            coalesce({{ metric }}, 0) as {{ metric }} {% if not loop.last %},{% endif %}
        {% endfor %}
    
    from {{ ref('stg_finviz__asset_attributes') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

grouped as (
    select
        desc_industry,
        min(mnt_market_capitalization) as min_mnt_market_capitalization,
        max(mnt_market_capitalization) as max_mnt_market_capitalization,

        {% for metric in metrics %}
            min({{ metric }}) AS min_{{ metric }},
            max({{ metric }}) AS max_{{ metric }} {% if not loop.last %},{% endif %}
        {% endfor %}

    from source
    group by desc_industry
),

normalized as (
    select
        s.desc_ticker,
        s.desc_industry,
        s.mnt_market_capitalization,

        {% for metric in metrics %}
            {{ metric }},
        {% endfor %}

        round(
            coalesce((s.mnt_market_capitalization - g.min_mnt_market_capitalization) / 
                nullif(g.max_mnt_market_capitalization - g.min_mnt_market_capitalization, 0), 0)
            , 4) :: double as rt_normalized_market_capitalization,

        {% for metric in metrics %}
            {% set metric_name = metric | replace('rt_', '') | replace('cod_', '') %}
            round(
                coalesce((g.max_{{ metric }} - s.{{ metric }}) / 
                    nullif(g.max_{{ metric }} - g.min_{{ metric }}, 0), 0)
                , 4) :: double as rt_normalized_{{ metric_name }},
        {% endfor %}

        s.mnt_target_price,
        s.mnt_close_price,
        s.audit_loaded_at

    from source s
    inner join grouped g
    on g.desc_industry = s.desc_industry

)

select * from normalized