WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','supplier_raw_data') }}

),

flattened AS (

SELECT
    f.value:supplier_id::STRING AS supplier_id,

    {{ clean_text("f.value:supplier_name::STRING") }} AS supplier_name,

    {{ clean_text("f.value:contact_information.contact_person::STRING") }}
    AS contact_person,

    {{ clean_email("f.value:contact_information.email::STRING") }} AS email,
    {{ mask_phone("f.value:contact_information.phone::STRING") }} AS phone,

    {{ clean_text("f.value:supplier_type::STRING") }} AS supplier_type,

    ARRAY_TO_STRING(f.value:categories_supplied, ', ')
    AS categories_supplied,

    f.value:payment_terms::STRING AS payment_terms,

    f.value:contract_details.contract_id::STRING AS contract_id,

    {{ parse_date("f.value:contract_details.start_date::STRING") }}
    AS contract_start_date,

    {{ parse_date("f.value:contract_details.end_date::STRING") }}
    AS contract_end_date,

    f.value:performance_metrics.on_time_delivery_rate::FLOAT
    AS on_time_delivery_rate,

    f.value:performance_metrics.average_delay_days::FLOAT
    AS average_delay_days,

    f.value:performance_metrics.defect_rate::FLOAT AS defect_rate,

    f.value:lead_time_days::INTEGER AS lead_time_days,

    f.value:credit_rating::STRING AS credit_rating,

    f.value:year_established::INTEGER AS year_established,

    {{ parse_date("f.value:last_order_date::STRING") }} AS last_order_date,

    f.value:is_active::BOOLEAN AS is_active,

    {{ parse_date("f.value:last_modified_date::STRING") }}
    AS last_modified_date

FROM source,
LATERAL FLATTEN(input => raw_data:suppliers_data) f

)

SELECT * FROM flattened