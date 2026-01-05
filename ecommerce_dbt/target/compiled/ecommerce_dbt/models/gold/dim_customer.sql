

SELECT 
    customer_unique_id as customer_id,
    customer_unique_id,
    UPPER(SUBSTR(city,1,1)) || LOWER(SUBSTR(city,2)) AS customer_city, --Viết hoa chữ đầu
    UPPER(state) AS customer_state        -- Viết hoa tất cả chữ
FROM "iceberg"."bronze_silver"."stg_customers"