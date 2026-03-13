WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','customers_raw') }}

),

flattened AS (

    SELECT
        f.value:customer_id::STRING AS customer_id,
        f.value:first_name::STRING AS first_name,
        f.value:last_name::STRING AS last_name,

        LOWER(f.value:email::STRING) AS email,

        f.value:phone::STRING AS phone,

        COALESCE(
                    TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
                    TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
                ) AS birth_date,
        COALESCE(
                    TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
                    TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
                )  AS registration_date,

        f.value:preferred_communication::STRING AS preferred_communication,
        f.value:occupation::STRING AS occupation,
        f.value:income_bracket::STRING AS income_bracket,
        f.value:loyalty_tier::STRING AS loyalty_tier,

        f.value:total_purchases::INTEGER AS total_purchases,
        f.value:total_spend::NUMBER(12,2) AS total_spend,

        f.value:preferred_payment_method::STRING AS preferred_payment_method,
        f.value:marketing_opt_in::BOOLEAN AS marketing_opt_in,

        COALESCE(
                    TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
                    TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
                ) AS last_purchase_date,
        COALESCE(
                    TRY_TO_DATE(f.value:birth_date::STRING,'YYYY-MM-DD'),
                    TRY_TO_DATE(f.value:birth_date::STRING,'DD-MM-YYYY')
                ) AS last_modified_date,

        /* Address Flattening */
        f.value:address.street::STRING AS street,
        f.value:address.city::STRING AS city,
        f.value:address.state::STRING AS state,
        f.value:address.zip_code::STRING AS zip_code,
        f.value:address.country::STRING AS country

    FROM source,
    LATERAL FLATTEN(input => raw_data:customers_data) f

)

SELECT * FROM flattened