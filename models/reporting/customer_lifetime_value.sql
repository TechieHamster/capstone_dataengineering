SELECT
    customer_id,
    SUM(gross_sales) AS lifetime_value
FROM {{ ref('fact_sales') }}
GROUP BY customer_id
ORDER BY lifetime_value DESC