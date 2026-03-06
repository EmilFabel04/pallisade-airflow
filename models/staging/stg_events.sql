{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw','events') }}
)

select
  -- In `bigen-484520.public.events`, the primary key column is `id` (not `event_id`).
  id as event_id,
  customer_id,
  event_name,
  event_category,
  session_id,
  page_url,
  device_type,
  browser,
  country,
  city,
  utm_source,
  utm_medium,
  utm_campaign,
  -- public.events.event_timestamp is STRING; normalize to TIMESTAMP
  parse_timestamp('%Y-%m-%dT%H:%M:%E*S%Ez', event_timestamp) as event_timestamp,
  -- public.events.revenue is STRING; normalize to FLOAT64
  safe_cast(revenue as float64) as revenue
from src
