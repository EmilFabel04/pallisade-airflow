{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('raw','events') }}
)

select
  -- Source uses `id` as the event identifier
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
  revenue as revenue,
  _source,
  _loaded_at,
  _batch_id
from src
