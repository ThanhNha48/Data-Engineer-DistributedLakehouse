{{ config(materialized = 'table', schema = 'silver') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,

    TRY_CAST(o.order_purchase_timestamp AS TIMESTAMP)            AS purchase_timestamp,
    TRY_CAST(o.order_approved_at AS TIMESTAMP)                   AS approved_timestamp,
    TRY_CAST(o.order_delivered_carrier_date AS TIMESTAMP)        AS carrier_timestamp,
    TRY_CAST(o.order_delivered_customer_date AS TIMESTAMP)       AS delivered_timestamp,
    TRY_CAST(o.order_estimated_delivery_date AS TIMESTAMP)       AS estimated_timestamp,

    CAST(oi.order_item_id AS INTEGER)                         AS item_sequence,
    oi.product_id,
    oi.seller_id,
    CAST(oi.price AS DOUBLE)                                  AS unit_price,
    CAST(oi.freight_value AS DOUBLE)                          AS freight_value,

    CAST(p.payment_sequential AS INTEGER)                     AS payment_seq,
    p.payment_type,
    CAST(p.payment_installments AS INTEGER)                  AS installments,
    CAST(p.payment_value AS DOUBLE)                           AS payment_amount,

    r.review_id,
    CAST(r.review_score AS INTEGER)                           AS review_score,
    r.review_comment_title,
    r.review_comment_message,
    TRY_CAST(r.review_creation_date AS TIMESTAMP)                AS review_creation_date,

    c.customer_unique_id

FROM {{ source('bronze', 'olist_orders_dataset') }} o
LEFT JOIN {{ source('bronze', 'olist_order_items_dataset') }} oi
    ON o.order_id = oi.order_id
LEFT JOIN {{ source('bronze', 'olist_order_payments_dataset') }} p
    ON o.order_id = p.order_id
LEFT JOIN {{ source('bronze', 'olist_order_reviews_dataset') }} r
    ON o.order_id = r.order_id
LEFT JOIN {{ source('bronze', 'olist_customers_dataset') }} c
    ON o.customer_id = c.customer_id
