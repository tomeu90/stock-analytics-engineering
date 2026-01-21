{{ config(
    materialized='incremental',
    unique_key=['sk_news_article', 'audit_loaded_at']
) }}

with 

source as (

    select 
        sk_news_article,
        desc_news_title,
        desc_news_source,
        dt_news_posting,
        desc_news_url,
        desc_news_category,
        desc_news_related_tickers,
        rt_negative_sentiment_score,
        rt_neutral_sentiment_score,
        rt_positive_sentiment_score,
        rt_compound_sentiment_score,
        desc_sentiment_score,
        rt_sentiment_score_certainty,
        audit_fetch_status,
        audit_fetched_at,
        audit_loaded_at

    from {{ ref('stg_finviz__news_scores') }}

    {% if is_incremental() %}
        where audit_loaded_at > (select coalesce(max(audit_loaded_at), 19700101) from {{ this }})
    {% endif %}
),

created as (
    select
        *,
        current_timestamp() as audit_created_at
    from source
)

select * from created