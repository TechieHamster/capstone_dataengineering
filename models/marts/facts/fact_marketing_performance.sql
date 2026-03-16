WITH sales AS (

    SELECT *
    FROM {{ ref('fact_sales') }}

),

campaigns AS (

    SELECT *
    FROM {{ ref('dim_campaign') }}

),

campaign_sales AS (

SELECT

    s.campaign_id,

    COUNT(DISTINCT s.order_id) AS total_orders,

    SUM(s.gross_sales) AS campaign_revenue,

    SUM(s.profit_amount) AS campaign_profit

FROM sales s
GROUP BY s.campaign_id

),

final AS (

SELECT

    c.campaign_id,

    c.campaign_name,
    c.campaign_type,
    c.channel,

    c.start_date,
    c.end_date,

    cs.total_orders,

    cs.campaign_revenue,

    c.total_cost AS campaign_cost,

    cs.campaign_profit,

    /* ROI */

    (cs.campaign_revenue - c.total_cost) /
    NULLIF(c.total_cost,0) AS campaign_roi

FROM campaigns c
LEFT JOIN campaign_sales cs
ON c.campaign_id = cs.campaign_id

)

SELECT * FROM final