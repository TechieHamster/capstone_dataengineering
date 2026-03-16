{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

select * 
FROM {{ ref('stg_customers') }} 

{% endsnapshot %}

--This tracks changes when last_modified_date changes