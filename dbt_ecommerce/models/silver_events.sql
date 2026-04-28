{{ config(materialized='table') }}


SELECT 
    CAST(event_time AS TIMESTAMP) AS event_time,
    event_type,
    product_id,
    user_id,
    user_session
FROM {{ source('public', 'raw_events') }}
WHERE user_id IS NOT NULL 
  AND product_id IN (SELECT product_id FROM {{ ref('silver_products') }})