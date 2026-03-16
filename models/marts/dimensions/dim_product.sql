WITH source AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

deduplicated AS (

SELECT *
FROM source
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id
    ORDER BY last_modified_date DESC 
) = 1

),

final AS (

SELECT
    product_id,

    product_name,
    short_description,
    technical_specs,

    category,
    subcategory,
    product_line,

    brand,
    color,
    size,

    unit_price,
    cost_price,
    profit_margin_percentage,

    supplier_id,

    stock_quantity,
    reorder_level,
    low_stock_flag,

    weight,
    dimensions,

    is_featured,

    launch_date,
    warranty_period,

    last_modified_date

FROM deduplicated

)

SELECT * FROM final