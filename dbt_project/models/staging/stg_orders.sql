-- stg_orders.sql
-- Staging model for orders: renames columns, casts timestamps,
-- and filters out test/invalid orders.
-- Source: staging.raw_orders (populated by Spark consumer)

with source as (
    select * from {{ source('staging', 'raw_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        purchase_timestamp::timestamp                as order_purchase_timestamp,
        approved_at::timestamp                       as order_approved_at,
        delivered_carrier_date::timestamp            as order_delivered_carrier_date,
        delivered_customer_date::timestamp           as order_delivered_customer_date,
        estimated_delivery_date::timestamp           as order_estimated_delivery_date,
        customer_city,
        customer_state,
        ingested_at
    from source
    where
        order_id    is not null
        and customer_id is not null
        and order_status in (
            'delivered', 'shipped', 'processing',
            'approved', 'invoiced', 'unavailable', 'canceled'
        )
),

final as (
    select
        *,
        -- Computed helper: days between purchase and delivery
        case
            when order_delivered_customer_date is not null
                 and order_purchase_timestamp  is not null
            then date_part(
                'day',
                order_delivered_customer_date - order_purchase_timestamp
            )
            else null
        end as delivery_days
    from renamed
)

select * from final
