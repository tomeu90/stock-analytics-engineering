with 

source as (

    select * from {{ ref('int_asset_news_deduplicated') }}

),

enriched as (
    select
        desc_news_title,
        regexp_replace(regexp_extract(desc_news_url, r'https?://([^/]+)', 1), r'^www\.', '') :: string as domain,
        dt_news_posting,
        desc_news_url,
        'Stock' :: string as desc_news_category,
        desc_news_related_tickers,
        audit_loaded_at

    from source
),

normalized as (
    select
        enr.desc_news_title,
        seed.source_name as desc_news_source,
        enr.dt_news_posting,
        enr.desc_news_url,
        enr.desc_news_category,
        enr.desc_news_related_tickers,
        enr.audit_loaded_at

    from enriched enr
    left join {{ ref('source_domains') }} seed
        on enr.domain = seed.domain
),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_news_url']) }} :: string as id_news_article,
        *

    from normalized    
)

select * from hashed