WITH source AS (

    SELECT *
    FROM {{ ref('stg_customers') }}

),

final AS (

SELECT
    customer_id,

    full_name,
    first_name,
    last_name,

    email,
    phone_number,

    age,
    customer_segment,

    loyalty_tier,

    city,
    state,
    country,

    total_purchases,
    total_spend,

    registration_date,
    last_purchase_date,

    last_modified_date

FROM source

)

SELECT * FROM final