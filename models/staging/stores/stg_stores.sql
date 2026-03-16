WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','store_raw_data') }}

),

flattened AS (

SELECT
    f.value:store_id::STRING AS store_id,

    {{ clean_text("f.value:store_name::STRING") }} AS store_name,

    {{ clean_text("f.value:address.street::STRING") }} AS street,
    {{ clean_text("f.value:address.city::STRING") }} AS city,
    f.value:address.state::STRING AS state,
    f.value:address.zip_code::STRING AS zip_code,
    f.value:address.country::STRING AS country,

    {{ clean_text("f.value:region::STRING") }} AS region,
    {{ clean_text("f.value:store_type::STRING") }} AS store_type,

    {{ parse_date("f.value:opening_date::STRING") }} AS opening_date,

    f.value:size_sq_ft::INTEGER AS size_sq_ft,

    CASE
        WHEN f.value:size_sq_ft::INTEGER < 5000 THEN 'Small'
        WHEN f.value:size_sq_ft::INTEGER BETWEEN 5000 AND 10000 THEN 'Medium'
        ELSE 'Large'
    END AS store_size_category,

    f.value:manager_id::STRING AS manager_id,

    {{ mask_phone("f.value:phone_number::STRING") }} AS phone,
    {{ clean_email("f.value:email::STRING") }} AS email,

    ARRAY_TO_STRING(f.value:services, ', ') AS services,

    f.value:employee_count::INTEGER AS employee_count,
    f.value:is_active::BOOLEAN AS is_active,

    f.value:monthly_rent::NUMBER(12,2) AS monthly_rent,

    f.value:sales_target::NUMBER(12,2) AS sales_target,
    f.value:current_sales::NUMBER(12,2) AS current_sales,

    (f.value:current_sales::NUMBER /
        NULLIF(f.value:sales_target::NUMBER,0)) * 100
    AS sales_target_achievement_percentage,

    (f.value:current_sales::NUMBER /
        NULLIF(f.value:size_sq_ft::NUMBER,0))
    AS revenue_per_sq_ft,

    {{ parse_date("f.value:last_modified_date::STRING") }} AS last_modified_date

FROM source,
LATERAL FLATTEN(input => raw_data:stores_data) f

)

SELECT * FROM flattened