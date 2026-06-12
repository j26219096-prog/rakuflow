-- dim_sellers.sql
-- Seller dimension enriched with aggregate metrics.
-- Computes total_orders and total_revenue from staging tables
-- (avoids circular dependency with fact_orders).

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Aggregate order metrics directly from staging (no circular dependency)
seller_metrics as (
    select
        oi.seller_id,
        count(distinct oi.order_id)            as total_orders,
        sum(coalesce(p.payment_value, 0))      as total_revenue
    from {{ source('staging', 'raw_order_items') }}  oi
    left join {{ source('staging', 'raw_payments') }} p on oi.order_id = p.order_id
    group by oi.seller_id
),

final as (
    select
        s.seller_key,
        s.seller_id,
        s.seller_city   as city,
        s.seller_state  as state,
        coalesce(sm.total_orders, 0)            as total_orders,
        round(coalesce(sm.total_revenue, 0)::numeric, 2) as total_revenue,
        s.dbt_updated_at
    from sellers        s
    left join seller_metrics sm on s.seller_id = sm.seller_id
)

select * from final
