{% snapshot asset_identity_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key = 'desc_ticker',
        strategy = 'check',
        hard_deletes = 'ignore',
        check_cols = [
            'desc_company',
            'desc_sector',
            'desc_industry',
            'desc_country',
            'desc_exchange',
            'desc_index'
        ]
    )
}}

with stage as (
    select
        desc_ticker,
        desc_company,
        desc_sector,
        desc_industry,
        desc_country,
        desc_exchange,
        desc_index,
        audit_loaded_at

    from {{ ref('stg_finviz__asset_attributes') }}
    where audit_loaded_at = (select max(audit_loaded_at) from {{ ref('stg_finviz__asset_attributes') }})
)

select
    *,
    current_timestamp() :: timestamp as audit_created_at

from stage

{% endsnapshot %}