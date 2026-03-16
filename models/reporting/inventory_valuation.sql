SELECT
    product_id,
    inventory_cost_value,
    inventory_retail_value
FROM {{ ref('fact_inventory') }}