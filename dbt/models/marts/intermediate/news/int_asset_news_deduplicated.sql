{{ config(
    materialized='incremental',
    unique_key=['desc_news_url', 'audit_loaded_at']
) }}

with 

source as (

    select 
        desc_news_title, 
        dt_news_posting, 
        desc_news_url,
        desc_ticker, 
        audit_loaded_at

    from {{ ref('stg_finviz__asset_attributes') }}
    where desc_news_title is not null

    {% if is_incremental() %}
        and audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

base as (

    select
        *,
        row_number() over (partition by desc_news_url order by dt_news_posting desc) as rn

    from source
),

canonical as (
    select *
    from base
    where rn = 1

),

tickers as (
    select
        desc_news_url,
        concat_ws(', ', collect_set(desc_ticker)) as desc_news_related_tickers
    from base
    group by desc_news_url

),

final as (
    select
        c.desc_news_title,
        c.dt_news_posting,
        c.desc_news_url,
        t.desc_news_related_tickers,
        c.audit_loaded_at
    from canonical c
    left join tickers t
        using (desc_news_url)

)

select * from final