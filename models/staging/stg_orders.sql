{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw','orders') }}
)

select
  -- Source uses `id` as the order identifier
  id as order_id,
  order_number,
  customer_email,
  customer_name,
  status,
  payment_status,
  payment_method,
  subtotal,
  tax,
  shipping,
  discount,
  total,
  items_count,
  shipping_country,
  shipping_state,
  shipping_city,
  -- public.orders created_at/updated_at are STRING; normalize to TIMESTAMP
  parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', created_at) as created_at,
  parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', updated_at) as updated_at,
  _source,
  _loaded_at,
  _batch_id
from src
