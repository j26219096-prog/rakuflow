-- stg_customers.sql
-- Staging model for customers: deduplicates, normalizes city names.
-- Source: staging.raw_orders (customer data enriched by Spark via customer CSV lookup)

with source as (
    select
        customer_id,
        customer_city,
        customer_state
    from {{ source('staging', 'raw_orders') }}
    where customer_id is not null
),

-- Normalize city names: trim whitespace and title-case
normalized as (
    select
        customer_id,
        initcap(trim(customer_city))  as customer_city,
        upper(trim(customer_state))   as customer_state
    from source
),

-- Deduplicate: one record per customer_id
deduped as (
    select distinct on (customer_id)
        customer_id,
        customer_city,
        customer_state
    from normalized
    order by customer_id, customer_city
),

final as (
    select
        md5(customer_id)              as customer_key,
        customer_id,
        customer_city,
        customer_state,
        current_timestamp as dbt_updated_at
    from deduped
)

select * from final
