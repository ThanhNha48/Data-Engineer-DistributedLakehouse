{{config(materialized='table', schema='gold')}}

SELECT
    product_id,
    category_name_en AS product_category_name,
    name_length AS product_name_length,
    description_length AS product_description_length,
    photos_qty AS product_photos_qty,
    weight_g AS product_weight_g,
    length_cm AS product_length_cm,
    height_cm AS product_height_cm,
    width_cm AS product_width_cm
FROM {{ ref('stg_products') }}