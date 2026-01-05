{% snapshot dim_sellers_scd2 %}

{{
    config(
        target_schema='silver',
        unique_key='seller_id',
        strategy='check',
        check_cols='all'
    )
}}

SELECT
    seller_id,
    CAST(seller_zip_code_prefix AS INTEGER) AS zip_code_prefix,
    LOWER(LEFT(seller_city, 1)) || UPPER(SUBSTR(seller_city, 2)) AS city,
    UPPER(seller_state) AS state
FROM {{ source('bronze', 'olist_sellers_dataset') }}

{% endsnapshot %}
