{{ config(
    materialized='incremental',
    unique_key=['order_id','product_id']
) }}

WITH orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        WHERE order_date >= (
            SELECT COALESCE(MAX(order_date),'1900-01-01')
            FROM {{ this }}
        )
    {% endif %}

),

deduplicated AS (

SELECT *
FROM orders
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id, product_id
    ORDER BY order_date DESC
) = 1

)

SELECT
    order_id,
    product_id,
    customer_id,
    store_id,
    employee_id,
    campaign_id,

    CAST(order_date AS DATE) AS order_date,

    quantity,
    unit_price,
    cost_price,

    gross_sales,
    total_cost,
    profit_amount,

    order_year,
    order_month,
    order_time_of_day

FROM deduplicated