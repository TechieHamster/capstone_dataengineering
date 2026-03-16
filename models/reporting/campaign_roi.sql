SELECT
    campaign_name,
    campaign_roi,
    campaign_revenue
FROM {{ ref('fact_marketing_performance') }}
ORDER BY campaign_roi DESC