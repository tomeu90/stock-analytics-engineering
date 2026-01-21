{{ config(
    materialized='view'
) }}

with 

company_news_source as (

    select * from {{ ref('int_asset_news_hashed') }}

),

market_news_source as (

    select * from {{ ref('int_market_news_hashed') }}

),

united as (
    select * from company_news_source
    union all
    select * from market_news_source
)

select 
    *, 
    current_timestamp() :: timestamp as audit_created_at 
from united