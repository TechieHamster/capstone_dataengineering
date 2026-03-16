SELECT
    p.category,
    SUM(f.gross_sales) AS total_sales
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} p
    ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC