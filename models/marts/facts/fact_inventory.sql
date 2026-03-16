WITH products AS (

    SELECT *
    FROM {{ ref('stg_products') }}

),

final AS (

SELECT

    product_id,
    supplier_id,

    stock_quantity,
    reorder_level,

    unit_price,
    cost_price,

    /* inventory metrics */

    stock_quantity * cost_price AS inventory_cost_value,

    stock_quantity * unit_price AS inventory_retail_value,

    CASE
        WHEN stock_quantity < reorder_level
        THEN TRUE
        ELSE FALSE
    END AS low_stock_flag,

    last_modified_date

FROM products

)

SELECT * FROM final