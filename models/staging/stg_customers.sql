-- Staging model using Databricks sample TPC-H data
-- Source: samples.tpch.customer

{{
  config(
    materialized='view'
  )
}}

select
    c_custkey as customer_id,
    c_name as customer_name,
    c_address as address,
    c_nationkey as nation_id,
    c_phone as phone,
    c_acctbal as account_balance,
    c_mktsegment as market_segment,
    c_comment as comment
from samples.tpch.customer

