-- agg_daily_gmv.sql
-- Daily GMV (Gross Merchandise Value) aggregate.
-- Grain: one row per calendar date.

with fact as (
    select * from {{ ref('fact_orders') }}
),

daily as (
    select
        order_purchase_timestamp::date    as order_date,
        count(distinct order_id)          as total_orders,
        sum(payment_value)                as total_gmv,
        avg(payment_value)                as avg_payment_value,
        sum(case when is_delivered then 1 else 0 end) as delivered_orders,
        avg(delivery_days)                as avg_delivery_days
    from fact
    where order_purchase_timestamp is not null
    group by 1
)

select
    order_date,
    total_orders,
    round(total_gmv::numeric, 2)          as total_gmv,
    round(avg_payment_value::numeric, 2)  as avg_payment_value,
    delivered_orders,
    round(avg_delivery_days::numeric, 1)  as avg_delivery_days
from daily
order by order_date
