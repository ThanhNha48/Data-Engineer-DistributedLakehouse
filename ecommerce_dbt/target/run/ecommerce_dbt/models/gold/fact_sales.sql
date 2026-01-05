
  
    

    create table "iceberg"."gold_gold"."fact_sales__dbt_tmp"
      
      
    as (
      

SELECT 
    o.order_id,                          -- PK
    o.product_id, 
    o.customer_unique_id AS customer_id,
    o.seller_id,

    CAST(
        date_format(o.purchase_timestamp, '%Y%m%d')
        AS INTEGER
    ) AS date_key,                       -- FK -> dim_date

    o.order_status, 
    o.unit_price AS price,
    o.freight_value,
    o.unit_price + o.freight_value AS total_price,
    o.payment_amount,
    o.review_score,
    o.purchase_timestamp AS order_purchase_timestamp

FROM "iceberg"."gold_silver"."stg_orders" o
WHERE o.product_id IS NOT NULL
    );

  