
  
    

    create table "iceberg"."gold_gold"."mart_top_products__dbt_tmp"
      
      
    as (
      

SELECT
    p.category_name_en, --Sản phẩm bán chạy nhất
    o.product_id,
    COUNT(*) AS quantity_sold,
    SUM(o.unit_price + o.freight_value) AS total_revenue
FROM "iceberg"."gold_silver"."stg_orders" o
LEFT JOIN "iceberg"."gold_silver"."stg_products" p ON o.product_id = p.product_id
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 50
    );

  