-- fact_orders.sql
-- Fact table: one row per order.
-- Joins staging orders with customer and seller dimensions.
-- Grain: order_id

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

sellers as (
    select * from {{ ref('dim_sellers') }}
),

-- Get seller_id per order from the order_items staging table
order_items as (
    select
        order_id,
        -- Take the first seller_id per order (primary seller)
        min(seller_id) as seller_id
    from {{ source('staging', 'raw_order_items') }}
    group by order_id
),

-- Get total payment per order
payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value
    from {{ source('staging', 'raw_payments') }}
    group by order_id
),

joined as (
    select
        o.order_id,
        c.customer_key,
        coalesce(s.seller_key, 'unknown')   as seller_key,
        coalesce(p.total_payment_value, 0)  as payment_value,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.delivery_days,
        (o.order_status = 'delivered')       as is_delivered,
        o.customer_state,
        o.ingested_at
    from orders        o
    left join customers    c on o.customer_id  = c.customer_id
    left join order_items  oi on o.order_id    = oi.order_id
    left join sellers      s on oi.seller_id   = s.seller_id
    left join payments     p on o.order_id     = p.order_id
)

select * from joined
