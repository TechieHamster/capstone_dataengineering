WITH orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

)
,

final AS (

SELECT

    /* identifiers */

    o.order_id,
    o.product_id,
    o.customer_id,
    o.store_id,
    o.employee_id,
    o.campaign_id,

    CAST(o.order_date AS DATE) AS order_date,

    /* metrics */

    o.quantity,

    o.unit_price,
    o.cost_price,

    o.gross_sales,
    o.total_cost,
    o.profit_amount,

    /* time fields */

    o.order_year,
    o.order_month,
    o.order_time_of_day

FROM orders o

)

SELECT * FROM final