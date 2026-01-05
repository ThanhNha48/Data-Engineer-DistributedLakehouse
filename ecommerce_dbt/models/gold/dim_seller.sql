{{config(materialized='table', schema='gold')}}

SELECT 
    seller_id,
    zip_code_prefix AS seller_zip_code_prefix,
    UPPER(SUBSTR(city, 1, 1)) || LOWER(SUBSTR(city, 2)) AS seller_city, --Viết hoa chữ đầu
    UPPER(state) AS seller_state        -- Viết hoa tất cả chữ
FROM {{ ref('stg_sellers') }}
