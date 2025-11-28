-- Staging model using Databricks sample TPC-H data
-- Source: samples.tpch.orders

{{
  config(
    materialized='view'
  )
}}

select
    o_orderkey as order_id,
    o_custkey as customer_id,
    o_orderstatus as order_status,
    o_totalprice as total_price,
    o_orderdate as order_date,
    o_orderpriority as order_priority,
    o_clerk as clerk,
    o_shippriority as ship_priority,
    o_comment as comment
from samples.tpch.orders

