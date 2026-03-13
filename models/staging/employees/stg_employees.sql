WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','employee_data_raw') }}

),

flattened AS (

    SELECT
        f.value:employee_id::STRING AS employee_id,

        INITCAP(f.value:first_name::STRING) AS first_name,
        INITCAP(f.value:last_name::STRING) AS last_name,

        CONCAT(
            INITCAP(f.value:first_name::STRING),' ',
            INITCAP(f.value:last_name::STRING)
        ) AS full_name,

        LOWER(f.value:email::STRING) AS email,

        f.value:phone::STRING AS phone,

        f.value:hire_date::DATE AS hire_date,
        f.value:date_of_birth::DATE AS date_of_birth,

        DATEDIFF(year, f.value:date_of_birth::DATE, CURRENT_DATE) AS age,

        INITCAP(f.value:role::STRING) AS role,
        INITCAP(f.value:department::STRING) AS department,

        f.value:work_location::STRING AS store_id,

        f.value:salary::NUMBER(12,2) AS salary,
        f.value:manager_id::STRING AS manager_id,

        UPPER(f.value:employment_status::STRING) AS employment_status,

        INITCAP(f.value:education::STRING) AS education,

        /* Address flattening */

        f.value:address.street::STRING AS street,
        f.value:address.city::STRING AS city,
        f.value:address.state::STRING AS state,
        f.value:address.zip_code::STRING AS zip_code,

        /* Sales performance */

        f.value:sales_target::NUMBER(12,2) AS sales_target,
        f.value:current_sales::NUMBER(12,2) AS current_sales,

        f.value:performance_rating::FLOAT AS performance_rating,

        (f.value:current_sales::NUMBER / NULLIF(f.value:sales_target::NUMBER,0)) * 100
        AS target_achievement_percentage,

        /* Certifications (array → string) */

        ARRAY_TO_STRING(f.value:certifications, ', ') AS certifications,

        f.value:last_modified_date::DATE AS last_modified_date

    FROM source,
    LATERAL FLATTEN(input => raw_data:employees_data) f

)

SELECT * FROM flattened