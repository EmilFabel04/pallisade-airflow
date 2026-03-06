{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw','orders') }}
)

select
  -- In `bigen-484520.public.orders`, the primary key column is `id` (not `order_id`).
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
  billing_country,
  billing_zip,
  shipping_name,
  shipping_address,
  -- public.orders created_at/updated_at are DATETIME; normalize to TIMESTAMP for downstream models
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from src
