with 

source as (

    select * from {{ source('finviz', 'news_data') }}

),

renamed as (

    select
        title :: string as desc_news_title,
        source :: string as desc_news_source,
        {{ parse_mixed_timestamps('date') }} :: timestamp as dt_news_posting,
        url :: string as desc_news_url,
        category :: string as desc_news_category,
        load_date :: integer as audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source

)

select * from renamed