with source as (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2000-01-01' as date)",
            end_date="cast('2030-12-31' as date)"
        )
    }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} :: string as sk_date,
        date_day :: timestamp as dt_date,
        extract(year from date_day) :: integer as dt_year,
        extract(quarter from date_day) :: integer as dt_quarter,
        extract(month from date_day) :: integer as dt_month,
        extract(day from date_day) :: integer as dt_day,
        to_char(date_day, 'yyyy-MM-dd') :: string as dt_string_date,
        extract(dow from date_day) :: integer as dt_day_of_week,
        extract(doy from date_day) :: integer as dt_day_of_year,
        case
            when extract(dow from date_day) in (0,6)
            then true
            else false
        end :: boolean as flag_is_weekend,
        current_timestamp() :: timestamp as audit_created_at

    from source

)

select * from renamed