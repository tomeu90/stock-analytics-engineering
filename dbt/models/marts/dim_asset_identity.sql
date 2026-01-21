with 

source as (

    select * from {{ ref('asset_identity_snapshot') }}

),

hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(['desc_ticker']) }} :: string as sk_asset_ticker,
        desc_ticker,
        desc_company,
        desc_sector,
        desc_industry,
        desc_country,
        desc_exchange,
        desc_index,
        audit_loaded_at,
        current_timestamp() :: timestamp as audit_created_at

    from source
    where dbt_valid_to is null
)

select * from hashed