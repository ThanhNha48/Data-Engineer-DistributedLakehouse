{% snapshot dim_customers_scd2 %}

{{
    config(
        target_schema='silver',
        unique_key='customer_unique_id',
        strategy='check',
        check_cols='all',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_unique_id,
    customer_id,
    CAST(customer_zip_code_prefix AS INTEGER) AS zip_code_prefix,
    UPPER(LEFT(customer_city, 1)) || LOWER(SUBSTR(customer_city, 2)) AS city,
    UPPER(customer_state) AS state
FROM {{ source('bronze', 'olist_customers_dataset') }}

{% endsnapshot %}
