{{ config(materialized='table') }}


WITH deduplicated_products AS (
    SELECT 
        product_id, 
        category_code, 
        brand, 
        price,
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY event_time DESC) as row_num
    FROM {{ source('public', 'raw_events') }}
    WHERE brand IS NOT NULL 
      AND price > 0
)
SELECT 
    product_id, 
    category_code, 
    brand, 
    price
FROM deduplicated_products
WHERE row_num = 1