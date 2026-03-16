WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','product_raw_data') }}

),

flattened AS (

    SELECT
        f.value:product_id::STRING AS product_id,

        {{ clean_text("f.value:name::STRING") }} AS product_name,

        {{ clean_text("f.value:short_description::STRING") }} AS short_description,
        {{ clean_text("f.value:technical_specs::STRING") }} AS technical_specs,

        /* Product hierarchy */

        {{ clean_text("f.value:category::STRING") }} AS category,
        {{ clean_text("f.value:subcategory::STRING") }} AS subcategory,
        {{ clean_text("f.value:product_line::STRING") }} AS product_line,

        {{ clean_text("f.value:brand::STRING") }} AS brand,
        {{ clean_text("f.value:color::STRING") }} AS color,

        {{ clean_text("f.value:size::STRING") }} AS size,

        f.value:unit_price::NUMBER(12,2) AS unit_price,
        f.value:cost_price::NUMBER(12,2) AS cost_price,

        /* Profit margin calculation */

        (
            (f.value:unit_price::NUMBER - f.value:cost_price::NUMBER)
            / NULLIF(f.value:unit_price::NUMBER,0)
        ) * 100 AS profit_margin_percentage,

        f.value:supplier_id::STRING AS supplier_id,

        f.value:stock_quantity::INTEGER AS stock_quantity,
        f.value:reorder_level::INTEGER AS reorder_level,

        /* Low stock flag */

        CASE
            WHEN f.value:stock_quantity::INTEGER < f.value:reorder_level::INTEGER
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag,

        {{ clean_text("f.value:weight::STRING") }} AS weight,
        {{ clean_text("f.value:dimensions::STRING") }} AS dimensions,

        f.value:is_featured::BOOLEAN AS is_featured,

        {{ parse_date("f.value:launch_date::STRING") }} AS launch_date,

        {{ clean_text("f.value:warranty_period::STRING") }} AS warranty_period,

        {{ parse_date("f.value:last_modified_date::STRING") }} AS last_modified_date

    FROM source,
    LATERAL FLATTEN(input => raw_data:products_data) f

)

SELECT * FROM flattened