WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','supplier_raw_data') }}

),

flattened AS (

    SELECT
        f.value:supplier_id::STRING AS supplier_id,
        INITCAP(f.value:supplier_name::STRING) AS supplier_name,

        /* Contact Information */

        INITCAP(f.value:contact_information.contact_person::STRING) AS contact_person,
        LOWER(f.value:contact_information.email::STRING) AS email,
        f.value:contact_information.phone::STRING AS phone,
        f.value:contact_information.address::STRING AS address,

        /* Supplier Details */

        INITCAP(f.value:supplier_type::STRING) AS supplier_type,

        ARRAY_TO_STRING(f.value:categories_supplied, ', ') AS categories_supplied,

        f.value:payment_terms::STRING AS payment_terms,

        /* Contract Details */

        f.value:contract_details.contract_id::STRING AS contract_id,
        f.value:contract_details.start_date::DATE AS contract_start_date,
        f.value:contract_details.end_date::DATE AS contract_end_date,
        f.value:contract_details.renewal_option::BOOLEAN AS renewal_option,
        f.value:contract_details.exclusivity::BOOLEAN AS exclusivity,

        /* Contract Duration */

        DATEDIFF(
            day,
            f.value:contract_details.start_date::DATE,
            f.value:contract_details.end_date::DATE
        ) AS contract_duration_days,

        /* Performance Metrics */

        f.value:performance_metrics.on_time_delivery_rate::FLOAT AS on_time_delivery_rate,
        f.value:performance_metrics.average_delay_days::FLOAT AS average_delay_days,
        f.value:performance_metrics.defect_rate::FLOAT AS defect_rate,
        f.value:performance_metrics.returns_percentage::FLOAT AS returns_percentage,
        INITCAP(f.value:performance_metrics.quality_rating::STRING) AS quality_rating,
        f.value:performance_metrics.response_time_hours::INTEGER AS response_time_hours,

        /* Logistics */

        f.value:lead_time_days::INTEGER AS lead_time_days,
        f.value:minimum_order_quantity::INTEGER AS minimum_order_quantity,

        INITCAP(f.value:preferred_carrier::STRING) AS preferred_carrier,

        /* Business Info */

        f.value:credit_rating::STRING AS credit_rating,
        f.value:tax_id::STRING AS tax_id,
        f.value:year_established::INTEGER AS year_established,

        f.value:website::STRING AS website,

        /* Supplier Age */

        YEAR(CURRENT_DATE) - f.value:year_established::INTEGER
        AS supplier_age_years,

        /* Activity Status */

        f.value:is_active::BOOLEAN AS is_active,

        f.value:last_order_date::DATE AS last_order_date,
        f.value:last_modified_date::DATE AS last_modified_date

    FROM source,
    LATERAL FLATTEN(input => raw_data:suppliers_data) f

)

SELECT * FROM flattened