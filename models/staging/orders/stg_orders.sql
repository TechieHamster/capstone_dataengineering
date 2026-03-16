

WITH source AS (

    SELECT raw_data
    FROM {{ source('bronze','orders_raw_data') }}

),

flattened AS (

SELECT
    f.value:order_id::STRING AS order_id,
    f.value:customer_id::STRING AS customer_id,

    {{ parse_date("f.value:order_date::STRING") }} AS order_date,

    f.value:store_id::STRING AS store_id,
    f.value:employee_id::STRING AS employee_id,
    f.value:campaign_id::STRING AS campaign_id,

    f.value:total_amount::NUMBER(12,2) AS total_amount,
    f.value:discount_amount::NUMBER(12,2) AS discount_amount,
    f.value:shipping_cost::NUMBER(12,2) AS shipping_cost,
    f.value:tax_amount::NUMBER(12,2) AS tax_amount,

    {{ clean_text("f.value:order_status::STRING") }} AS order_status,

    {{ clean_text("f.value:payment_method::STRING") }} AS payment_method,
    {{ clean_text("f.value:shipping_method::STRING") }} AS shipping_method,

    CASE
    WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) 
        BETWEEN 5 AND 11 THEN 'Morning'

    WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) 
        BETWEEN 12 AND 16 THEN 'Afternoon'

    WHEN DATE_PART(hour, TRY_TO_TIMESTAMP(f.value:order_date::STRING)) 
        BETWEEN 17 AND 21 THEN 'Evening'

    ELSE 'Night'
END AS order_time_of_day,

    YEAR({{ parse_date("f.value:order_date::STRING") }}) AS order_year,
    MONTH({{ parse_date("f.value:order_date::STRING") }}) AS order_month,

    {{ parse_date("f.value:shipping_date::STRING") }} AS shipping_date,
    {{ parse_date("f.value:delivery_date::STRING") }} AS delivery_date,
    {{ parse_date("f.value:estimated_delivery_date::STRING") }}
    AS estimated_delivery_date

FROM source,
LATERAL FLATTEN(input => raw_data:orders_data) f

)

SELECT * FROM flattened