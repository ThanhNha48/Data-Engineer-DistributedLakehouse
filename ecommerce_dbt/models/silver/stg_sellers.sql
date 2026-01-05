{{ config(materialized='table', schema='silver') }}

SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS INTEGER) AS zip_code_prefix,
    LOWER(SUBSTR(seller_city, 1, 1)) || UPPER(SUBSTR(seller_city, 2)) AS city,
    UPPER(seller_state) AS state
FROM {{ source('bronze', 'olist_sellers_dataset') }}
