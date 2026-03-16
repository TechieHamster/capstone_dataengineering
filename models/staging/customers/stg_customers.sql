WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','customers_raw') }}

),

flattened AS (

    SELECT
        f.value:customer_id::STRING AS customer_id,

        {{ clean_text("f.value:first_name::STRING") }} AS first_name,
        {{ clean_text("f.value:last_name::STRING") }} AS last_name,

        {{ clean_email("f.value:email::STRING") }} AS email,

        {{ mask_phone("f.value:phone::STRING") }} AS phone,

        {{ parse_date("f.value:birth_date::STRING") }} AS birth_date,
        {{ parse_date("f.value:registration_date::STRING") }} AS registration_date,

        {{ clean_text("f.value:preferred_communication::STRING") }} AS preferred_communication,
        {{ clean_text("f.value:occupation::STRING") }} AS occupation,
        {{ clean_text("f.value:income_bracket::STRING") }} AS income_bracket,
        {{ clean_text("f.value:loyalty_tier::STRING") }} AS loyalty_tier,

        f.value:total_purchases::INTEGER AS total_purchases,
        f.value:total_spend::NUMBER(12,2) AS total_spend,

        {{ clean_text("f.value:preferred_payment_method::STRING") }} AS preferred_payment_method,
        f.value:marketing_opt_in::BOOLEAN AS marketing_opt_in,

        {{ parse_date("f.value:last_purchase_date::STRING") }} AS last_purchase_date,
        {{ parse_date("f.value:last_modified_date::STRING") }} AS last_modified_date,

        /* Address Flattening */

        {{ clean_text("f.value:address.street::STRING") }} AS street,
        {{ clean_text("f.value:address.city::STRING") }} AS city,
        f.value:address.state::STRING AS state,
        f.value:address.zip_code::STRING AS zip_code,
        {{ clean_text("f.value:address.country::STRING") }} AS country

    FROM source,
    LATERAL FLATTEN(input => raw_data:customers_data) f

)

SELECT * FROM flattened