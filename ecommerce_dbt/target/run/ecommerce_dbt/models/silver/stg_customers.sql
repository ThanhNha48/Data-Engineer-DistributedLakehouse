
  
    

    create table "iceberg"."gold_silver"."stg_customers__dbt_tmp"
      
      
    as (
      

SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS INTEGER) AS zip_code_prefix,
    UPPER(SUBSTR(customer_city, 1, 1)) || LOWER(SUBSTR(customer_city, 2)) AS city,
    UPPER(customer_state) AS state
FROM "iceberg"."bronze"."olist_customers_dataset"
    );

  