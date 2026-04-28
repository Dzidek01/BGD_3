{{ config(materialized='table') }}


SELECT 
    p.brand,
    COUNT(CASE WHEN e.event_type = 'view' THEN 1 END) AS total_views,
    COUNT(CASE WHEN e.event_type = 'cart' THEN 1 END) AS total_adds_to_cart,
    COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) AS total_purchases
FROM {{ ref('silver_events') }} e
JOIN {{ ref('silver_products') }} p ON e.product_id = p.product_id
GROUP BY p.brand
HAVING COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) > 50
ORDER BY total_purchases DESC