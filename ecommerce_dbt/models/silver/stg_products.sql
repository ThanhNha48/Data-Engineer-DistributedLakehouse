{{ config(materialized = 'table', schema = 'silver') }}

SELECT
    p.product_id,
    p.product_category_name                       AS category_name_pt,
    COALESCE(t.product_category_name_english,
             p.product_category_name)             AS category_name_en,
    CAST(p.product_name_lenght AS INTEGER)        AS name_length,
    CAST(p.product_description_lenght AS INTEGER) AS description_length,
    CAST(p.product_photos_qty AS INTEGER)         AS photos_qty,
    CAST(p.product_weight_g AS DOUBLE)            AS weight_g,
    CAST(p.product_length_cm AS DOUBLE)           AS length_cm,
    CAST(p.product_height_cm AS DOUBLE)           AS height_cm,
    CAST(p.product_width_cm AS DOUBLE)            AS width_cm
FROM {{ source('bronze', 'olist_products_dataset') }} p
LEFT JOIN {{ source('bronze', 'product_category_name_translation') }} t
    ON LOWER(p.product_category_name) = LOWER(t.product_category_name)
