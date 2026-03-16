SELECT
    p.product_id,
    p.product_name AS product_name,
    SUM(f.quantity) AS total_units_sold,
    SUM(f.gross_sales) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} p
    ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_units_sold DESC