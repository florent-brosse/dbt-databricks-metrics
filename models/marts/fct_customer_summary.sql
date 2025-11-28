-- Customer summary for metric view testing
-- Aggregated customer metrics

{{
  config(
    materialized='table'
  )
}}

select
    c.customer_id,
    c.customer_name,
    c.market_segment,
    c.account_balance,
    count(o.order_id) as total_orders,
    sum(o.total_price) as total_spent,
    avg(o.total_price) as avg_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o
    on c.customer_id = o.customer_id
group by 1, 2, 3, 4

