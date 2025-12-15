with 

source as (

    select * from {{ source('finviz', 'news_data') }}

),

renamed as (

    select
        title,
        source,
        date,
        url,
        category,
        load_date

    from source

)

select * from renamed