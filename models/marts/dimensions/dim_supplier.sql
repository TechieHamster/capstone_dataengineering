WITH source AS (

    SELECT *
    FROM {{ ref('stg_suppliers') }}

),

deduplicated AS (

SELECT *
FROM source
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY supplier_id
    ORDER BY last_modified_date DESC
) = 1

),

final AS (

SELECT
    supplier_id,

    supplier_name,

    contact_person,
    email,
    phone,

    supplier_type,

    categories_supplied,

    payment_terms,

    contract_id,
    contract_start_date,
    contract_end_date,

    on_time_delivery_rate,
    average_delay_days,
    defect_rate,

    lead_time_days,

    credit_rating,

    year_established,

    last_order_date,

    is_active,

    last_modified_date

FROM deduplicated

)

SELECT * FROM final