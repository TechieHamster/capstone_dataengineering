

WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','orders_raw_data') }}

),

orders_flattened AS (

    SELECT
        f.value:order_id::STRING AS order_id,
        f.value:customer_id::STRING AS customer_id,

        f.value:order_date::TIMESTAMP AS order_date,

        f.value:total_amount::NUMBER(12,2) AS total_amount,
        f.value:discount_amount::NUMBER(12,2) AS order_discount,
        f.value:shipping_cost::NUMBER(12,2) AS shipping_cost,
        f.value:tax_amount::NUMBER(12,2) AS tax_amount,

        INITCAP(f.value:order_status::STRING) AS order_status,
        INITCAP(f.value:payment_method::STRING) AS payment_method,
        INITCAP(f.value:shipping_method::STRING) AS shipping_method,

        /* shipping address */

        INITCAP(f.value:shipping_address.city::STRING) AS shipping_city,
        f.value:shipping_address.state::STRING AS shipping_state,
        f.value:shipping_address.zip_code::STRING AS shipping_zip_code,

        /* billing address */

        INITCAP(f.value:billing_address.city::STRING) AS billing_city,
        f.value:billing_address.state::STRING AS billing_state,
        f.value:billing_address.zip_code::STRING AS billing_zip_code,

        f.value:store_id::STRING AS store_id,
        f.value:employee_id::STRING AS employee_id,
        f.value:campaign_id::STRING AS campaign_id,

        f.value:order_source::STRING AS order_source,

        f.value:shipping_date::TIMESTAMP AS shipping_date,
        f.value:delivery_date::TIMESTAMP AS delivery_date,
        f.value:estimated_delivery_date::TIMESTAMP AS estimated_delivery_date,

        f.value:created_at::TIMESTAMP AS created_at,

        f.value:order_items AS order_items

    FROM source,
    LATERAL FLATTEN(input => raw_data:orders_data) f

),

items_flattened AS (

    SELECT
        o.*,

        i.value:product_id::STRING AS product_id,
        i.value:quantity::INTEGER AS quantity,

        i.value:unit_price::NUMBER(12,2) AS unit_price,
        i.value:cost_price::NUMBER(12,2) AS cost_price,

        i.value:discount_amount::NUMBER(12,2) AS item_discount

    FROM orders_flattened o,
    LATERAL FLATTEN(input => o.order_items) i

),

final AS (

    SELECT
        *,

        /* revenue calculation */

        quantity * unit_price AS gross_sales,

        quantity * cost_price AS total_cost,

        (quantity * unit_price) - (quantity * cost_price)
        AS profit_amount,

        /* delivery metrics */

        DATEDIFF(day, order_date, delivery_date)
        AS delivery_days,

        CASE
            WHEN delivery_date <= estimated_delivery_date
            THEN 'On Time'
            ELSE 'Delayed'
        END AS delivery_status

    FROM items_flattened

)

SELECT * FROM final