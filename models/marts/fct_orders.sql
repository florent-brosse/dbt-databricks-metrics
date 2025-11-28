-- Fact table for orders with customer information
-- This model will have metric views defined

{{
  config(
    materialized='table'
  )
}}

select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.market_segment,
    o.order_status,
    o.total_price,
    o.order_date,
    o.order_priority,
    year(o.order_date) as order_year,
    month(o.order_date) as order_month,
    quarter(o.order_date) as order_quarter
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id

