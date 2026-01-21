{{ config(
    materialized='incremental',
    unique_key=['sk_news_article', 'audit_loaded_at']
) }}

with 

source as (

    select * from {{ ref('stg_finviz__market_news') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_news_url']) }} :: string as sk_news_article,
        desc_news_title,
        desc_news_source,
        dt_news_posting,
        desc_news_url,
        desc_news_category,
        '' :: string as desc_news_related_tickers,
        audit_loaded_at

    from source
)

select * from hashed