-- dim_customers.sql
-- Customer dimension: SCD Type 1 (overwrite on update).
-- One row per customer_id.

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_key,
    customer_id,
    customer_city   as city,
    customer_state  as state,
    dbt_updated_at
from customers
