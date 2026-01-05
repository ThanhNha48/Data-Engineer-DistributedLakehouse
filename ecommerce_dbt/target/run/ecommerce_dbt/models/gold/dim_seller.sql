
  
    

    create table "iceberg"."gold_gold"."dim_seller__dbt_tmp"
      
      
    as (
      

SELECT 
    seller_id,
    zip_code_prefix AS seller_zip_code_prefix,
    UPPER(SUBSTR(city, 1, 1)) || LOWER(SUBSTR(city, 2)) AS seller_city, --Viết hoa chữ đầu
    UPPER(state) AS seller_state        -- Viết hoa tất cả chữ
FROM "iceberg"."gold_silver"."stg_sellers"
    );

  