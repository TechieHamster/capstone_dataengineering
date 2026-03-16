WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','customers_raw') }}

),

flattened AS (

SELECT
    f.value:customer_id::STRING AS customer_id,

    INITCAP(TRIM(f.value:first_name::STRING)) AS first_name,
    INITCAP(TRIM(f.value:last_name::STRING)) AS last_name,

    CONCAT(
        INITCAP(TRIM(f.value:first_name::STRING)),
        ' ',
        INITCAP(TRIM(f.value:last_name::STRING))
    ) AS full_name,

    LOWER(TRIM(f.value:email::STRING)) AS email,

    REGEXP_REPLACE(f.value:phone::STRING,'[^0-9]','') AS phone_number,

    /* handle multiple date formats */

    COALESCE(
        TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
        TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
    ) AS birth_date,

    TRY_TO_DATE(f.value:registration_date::STRING) AS registration_date,

    /* customer age */

    DATEDIFF(year,
        COALESCE(
            TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
            TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
        ),
        CURRENT_DATE
    ) AS age,

    /* customer segment */

    CASE
        WHEN DATEDIFF(year,
            TRY_TO_DATE(f.value:birth_date::STRING),
            CURRENT_DATE) BETWEEN 18 AND 35 THEN 'Young'

        WHEN DATEDIFF(year,
            TRY_TO_DATE(f.value:birth_date::STRING),
            CURRENT_DATE) BETWEEN 36 AND 55 THEN 'Middle Aged'

        ELSE 'Senior'
    END AS customer_segment,

    /* address */

    INITCAP(f.value:address.street::STRING) AS street,
    INITCAP(f.value:address.city::STRING) AS city,
    f.value:address.state::STRING AS state,
    f.value:address.zip_code::STRING AS zip_code,
    f.value:address.country::STRING AS country,

    f.value:loyalty_tier::STRING AS loyalty_tier,
    f.value:total_purchases::INTEGER AS total_purchases,
    f.value:total_spend::NUMBER(12,2) AS total_spend,

    TRY_TO_DATE(f.value:last_purchase_date::STRING) AS last_purchase_date,
    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date

FROM source,
LATERAL FLATTEN(input => raw_data:customers_data) f

)

SELECT * FROM flattened