{{ config(
    materialized='incremental',
    unique_key=['desc_news_related_tickers', 'audit_loaded_at']
) }}

with 

source as (

    select  
        desc_news_related_tickers,
        rt_compound_sentiment_score,
        rt_sentiment_score_certainty,
        audit_loaded_at

    from {{ ref('stg_finviz__news_scores') }}
    where desc_news_category = 'Stock'
    and audit_fetch_status = 'Success'

    {% if is_incremental() %}
        and audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}

),

exploded as (
    select
        trim(ticker) as desc_news_related_tickers,
        rt_compound_sentiment_score,
        rt_sentiment_score_certainty,
        audit_loaded_at,
        current_timestamp() as audit_created_at

    from source
    lateral view explode(split(desc_news_related_tickers, ',')) t as ticker
)

select * from exploded