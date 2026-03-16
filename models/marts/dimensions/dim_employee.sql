WITH source AS (

    SELECT *
    FROM {{ ref('stg_employees') }}

),

deduplicated AS (

SELECT *
FROM source
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY employee_id
    ORDER BY last_modified_date DESC
) = 1

),

final AS (

SELECT
    employee_id,

    first_name,
    last_name,
    full_name,

    email,
    phone,

    hire_date,
    date_of_birth,

    tenure_years,

    role,
    department,

    store_id,

    salary,
    manager_id,

    employment_status,

    education,

    street,
    city,
    state,
    zip_code,

    sales_target,
    current_sales,
    target_achievement_percentage,

    certifications,

    last_modified_date

FROM deduplicated

)

SELECT * FROM final