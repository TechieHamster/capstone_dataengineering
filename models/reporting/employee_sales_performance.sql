SELECT
    e.employee_id,
    e.full_name,
    SUM(f.gross_sales) AS total_sales
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_employee') }} e
    ON f.employee_id = e.employee_id
GROUP BY e.employee_id, e.full_name
ORDER BY total_sales DESC