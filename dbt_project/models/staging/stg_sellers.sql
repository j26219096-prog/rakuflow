-- stg_sellers.sql
-- Staging model for sellers: deduplicates and adds seller_state.
-- Source: staging.raw_sellers (populated from olist_sellers_dataset.csv via seed or Spark)

with source as (
    select * from {{ source('staging', 'raw_sellers') }}
    where seller_id is not null
),

normalized as (
    select
        seller_id,
        seller_zip_code_prefix,
        initcap(trim(seller_city))   as seller_city,
        upper(trim(seller_state))    as seller_state
    from source
),

deduped as (
    select distinct on (seller_id)
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from normalized
    order by seller_id
),

final as (
    select
        md5(seller_id)                as seller_key,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        current_timestamp as dbt_updated_at
    from deduped
)

select * from final
