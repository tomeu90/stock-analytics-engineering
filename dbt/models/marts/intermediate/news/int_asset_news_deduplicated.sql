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

),

ranked as (
    select 
        desc_news_title, 
        dt_news_posting, 
        desc_news_url, 
        desc_ticker,
        audit_loaded_at,
        row_number() over (partition by desc_news_url, desc_ticker order by dt_news_posting) as rn

    from source
),

deduplicated as (
    select 
        desc_news_title, 
        dt_news_posting, 
        desc_news_url,
        concat_ws(', ', collect_set(desc_ticker)) AS desc_news_related_tickers,
        audit_loaded_at

    from ranked
    where rn = 1
    group by desc_news_title, dt_news_posting, desc_news_url, audit_loaded_at
)

select * from deduplicated