WITH source AS(
        SELECT *
        FROM {{ ref('stg_stores') }}

),

deduplicated AS(

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() over(
        PARTITION BY store_id ORDER BY last_modified_date DESC
    ) = 1
),


final AS (

SELECT
    store_id,
    store_name,

    street,
    city,
    state,
    zip_code,
    country,

    region,
    store_type,

    opening_date,

    size_sq_ft,
    store_size_category,

    manager_id,

    phone,
    email,

    services,

    employee_count,
    is_active,

    monthly_rent,

    sales_target,
    current_sales,
    sales_target_achievement_percentage,

    revenue_per_sq_ft,

    last_modified_date

FROM deduplicated

)

SELECT * FROM final