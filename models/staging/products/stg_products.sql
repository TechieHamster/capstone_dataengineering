WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','product_raw_data') }}

),

flattened AS (

    SELECT
        f.value:product_id::STRING AS product_id,

        INITCAP(f.value:name::STRING) AS product_name,

        f.value:short_description::STRING AS short_description,
        f.value:technical_specs::STRING AS technical_specs,

        INITCAP(f.value:category::STRING) AS category,
        INITCAP(f.value:subcategory::STRING) AS subcategory,
        INITCAP(f.value:product_line::STRING) AS product_line,

        INITCAP(f.value:brand::STRING) AS brand,
        INITCAP(f.value:color::STRING) AS color,

        f.value:size::STRING AS size,

        f.value:unit_price::NUMBER(12,2) AS unit_price,
        f.value:cost_price::NUMBER(12,2) AS cost_price,

        /* Profit margin calculation */

        ((f.value:unit_price::NUMBER - f.value:cost_price::NUMBER)
         / NULLIF(f.value:unit_price::NUMBER,0)) * 100
        AS profit_margin_percentage,

        f.value:supplier_id::STRING AS supplier_id,

        f.value:stock_quantity::INTEGER AS stock_quantity,
        f.value:reorder_level::INTEGER AS reorder_level,

        /* Low stock indicator */

        CASE
            WHEN f.value:stock_quantity::INTEGER < f.value:reorder_level::INTEGER
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag,

        f.value:weight::STRING AS weight,
        f.value:dimensions::STRING AS dimensions,

        f.value:is_featured::BOOLEAN AS is_featured,

        f.value:launch_date::DATE AS launch_date,
        f.value:warranty_period::STRING AS warranty_period,

        f.value:last_modified_date::DATE AS last_modified_date

    FROM source,
    LATERAL FLATTEN(input => raw_data:products_data) f

)

SELECT * FROM flattened