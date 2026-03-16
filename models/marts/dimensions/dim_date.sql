WITH date_spine AS(
    SELECT 
        DATEADD(
            day, row_number() OVER (ORDER BY seq4()) - 1,
            '2020-01-01'
        ) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
),

final AS(
    SELECT 
        date_day AS date_key,
        YEAR(date_day) AS year,
        MONTH(date_day) AS month,
        DAY(date_day) AS day,

        MONTHNAME(date_day) AS month_name,
    DAYNAME(date_day) AS day_name,

    WEEK(date_day) AS week_of_year,
    QUARTER(date_day) AS quarter,

    CASE
        WHEN DAYOFWEEK(date_day) IN (6,7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM date_spine

)

SELECT * FROM final