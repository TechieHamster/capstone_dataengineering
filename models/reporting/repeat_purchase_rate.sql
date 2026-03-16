SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders
FROM {{ ref('fact_sales') }}
GROUP BY customer_id