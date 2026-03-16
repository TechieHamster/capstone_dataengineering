SELECT
    p.product_id,
    p.product_name,
    SUM(f.quantity) AS total_sales,
    CASE
        WHEN SUM(f.quantity) > 500 THEN 'Fast Moving'
        WHEN SUM(f.quantity) BETWEEN 100 AND 500 THEN 'Moderate'
        ELSE 'Slow Moving'
    END AS movement_category
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} p
    ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name