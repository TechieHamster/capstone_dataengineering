WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','orders_raw_data') }}

),

orders_flattened AS (

SELECT
    f.value:order_id::STRING AS order_id,
    f.value:customer_id::STRING AS customer_id,

    TRY_TO_TIMESTAMP(f.value:order_date::STRING) AS order_date,

    f.value:store_id::STRING AS store_id,
    f.value:employee_id::STRING AS employee_id,
    f.value:campaign_id::STRING AS campaign_id,

    f.value:total_amount::NUMBER(12,2) AS total_amount,
    f.value:discount_amount::NUMBER(12,2) AS order_discount,
    f.value:shipping_cost::NUMBER(12,2) AS shipping_cost,
    f.value:tax_amount::NUMBER(12,2) AS tax_amount,

    {{ clean_text("f.value:order_status::STRING") }} AS order_status,
    {{ clean_text("f.value:payment_method::STRING") }} AS payment_method,
    {{ clean_text("f.value:shipping_method::STRING") }} AS shipping_method,

    CASE
        WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS order_time_of_day,

    YEAR(TRY_TO_TIMESTAMP(f.value:order_date::STRING)) AS order_year,
    MONTH(TRY_TO_TIMESTAMP(f.value:order_date::STRING)) AS order_month,

    TRY_TO_DATE(f.value:shipping_date::STRING) AS shipping_date,
    TRY_TO_DATE(f.value:delivery_date::STRING) AS delivery_date,
    TRY_TO_DATE(f.value:estimated_delivery_date::STRING) AS estimated_delivery_date,

    f.value:order_items AS order_items

FROM source,
LATERAL FLATTEN(input => raw_data:orders_data) f

),

items_flattened AS (

SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.employee_id,
    o.campaign_id,
    o.order_date,
    o.order_year,
    o.order_month,

    o.shipping_date,
    o.delivery_date,
    o.estimated_delivery_date,

    o.order_status,
    o.payment_method,
    o.shipping_method,
    o.order_time_of_day,

    i.value:product_id::STRING AS product_id,
    i.value:quantity::INTEGER AS quantity,

    i.value:unit_price::NUMBER(12,2) AS unit_price,
    i.value:cost_price::NUMBER(12,2) AS cost_price,

    (i.value:quantity::NUMBER * i.value:unit_price::NUMBER) AS gross_sales,
    (i.value:quantity::NUMBER * i.value:cost_price::NUMBER) AS total_cost,

    (i.value:quantity::NUMBER * i.value:unit_price::NUMBER)
    -
    (i.value:quantity::NUMBER * i.value:cost_price::NUMBER)
    AS profit_amount

FROM orders_flattened o,
LATERAL FLATTEN(input => o.order_items) i

)

SELECT * FROM items_flattened