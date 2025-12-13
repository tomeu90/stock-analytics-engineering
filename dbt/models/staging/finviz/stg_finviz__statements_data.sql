with 

source as (

    select * from {{ source('finviz', 'statements_data') }}

),

renamed as (

    select
        load_date,
        ticker,
        period,
        statement,
        timeframe,
        line_item,
        value

    from source

)

select * from renamed