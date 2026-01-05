

SELECT
    DATE_TRUNC('month', purchase_timestamp) AS month, --Doanh thu theo tháng
    c.state,
    p.category_name_en,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(unit_price + freight_value) AS gross_revenue,
    SUM(payment_amount) AS net_revenue
FROM "iceberg"."bronze_silver"."stg_orders" o
LEFT JOIN "iceberg"."bronze_silver"."stg_customers" c ON o.customer_unique_id = c.customer_unique_id
LEFT JOIN "iceberg"."bronze_silver"."stg_products" p ON o.product_id = p.product_id
GROUP BY 1, 2, 3
ORDER BY month DESC, gross_revenue DESC
--