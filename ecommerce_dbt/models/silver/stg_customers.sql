{{ config(materialized = 'table', schema = 'silver') }}

SELECT
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS INTEGER) AS zip_code_prefix,
    UPPER(SUBSTR(customer_city, 1, 1)) || LOWER(SUBSTR(customer_city, 2)) AS city,
    UPPER(customer_state) AS state
FROM {{ source('bronze', 'olist_customers_dataset') }}
