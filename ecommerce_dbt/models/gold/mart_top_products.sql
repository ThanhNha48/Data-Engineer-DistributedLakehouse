{{ config(materialized='table', schema='gold') }}

SELECT
    p.category_name_en, --Sản phẩm bán chạy nhất
    o.product_id,
    COUNT(*) AS quantity_sold,
    SUM(o.unit_price + o.freight_value) AS total_revenue
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 50