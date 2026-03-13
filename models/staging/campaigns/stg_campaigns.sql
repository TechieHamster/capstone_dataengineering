WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','campaign_data_raw') }}

),

flattened AS (

    SELECT
    f.value:campaign_id::STRING        AS campaign_id,
    f.value:campaign_name::STRING      AS campaign_name,
    f.value:campaign_type::STRING      AS campaign_type,
    f.value:channel::STRING            AS channel,
    f.value:description::STRING        AS description,

    TO_TIMESTAMP(f.value:start_date::STRING) AS start_date,
    TO_TIMESTAMP(f.value:end_date::STRING)   AS end_date,

    TRY_TO_NUMBER(REPLACE(REPLACE(f.value:budget::STRING,'$',''),',',''))
        AS budget,

    TRY_TO_NUMBER(REPLACE(REPLACE(f.value:total_cost::STRING,'$',''),',',''))
        AS total_cost,

    TRY_TO_NUMBER(REPLACE(REPLACE(f.value:total_revenue::STRING,'$',''),',',''))
        AS total_revenue,

    f.value:roi_calculation::FLOAT     AS roi_calculation,
    f.value:target_audience::STRING    AS target_audience,

    TRY_TO_DATE(f.value:last_modified_date::STRING) AS last_modified_date

    FROM source,
    LATERAL FLATTEN(input => raw_data:campaigns_data) f

)

SELECT * FROM flattened