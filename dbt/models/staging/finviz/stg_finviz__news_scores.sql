with 

source as (

    select * from {{ source('finviz', 'news_scores_data') }}

),

renamed as (

    select
        sk_news_article :: string,
        desc_news_title :: string,
        desc_news_source :: string,
        dt_news_posting :: timestamp,
        desc_news_url :: string,
        desc_news_category :: string,
        desc_news_related_tickers :: string,
        desc_article_text :: string,
        round(rt_negative_sentiment_score, 4) :: double as rt_negative_sentiment_score,
        round(rt_neutral_sentiment_score, 4) :: double as rt_neutral_sentiment_score,
        round(rt_positive_sentiment_score, 4) :: double as rt_positive_sentiment_score,
        round(rt_compound_sentiment_score, 4) :: double as rt_compound_sentiment_score,
        desc_sentiment_score :: string,
        round(rt_sentiment_score_certainty, 4) :: double as rt_sentiment_score_certainty,
        audit_fetch_status :: string,
        audit_fetched_at :: timestamp,
        audit_loaded_at :: integer,
        audit_created_at :: timestamp

    from source

)

select * from renamed