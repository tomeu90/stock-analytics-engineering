with 

source as (

    select * from {{ source('finviz', 'statements_data') }}
    where period != 'TTM'

),

renamed as (

    select
        ticker :: string as desc_ticker,
        substr(period, 0, 4) :: integer as dt_year,
        substr(period, 5, 6) :: string as dt_period,
        timeframe :: string as dt_timeframe,
        statement :: string as desc_financial_statement,
        line_item :: string as desc_line_item,
        value :: string as mnt_line_item_value,
        load_date :: integer as audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source

)

select * from renamed