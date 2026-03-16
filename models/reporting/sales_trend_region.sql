SELECT
    s.region,
    d.year,
    d.month,
    SUM(f.gross_sales) AS total_sales
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_store') }} s
    ON f.store_id = s.store_id
JOIN {{ ref('dim_date') }} d
    ON f.order_date = d.date_key
GROUP BY s.region, d.year, d.month
ORDER BY d.year, d.month