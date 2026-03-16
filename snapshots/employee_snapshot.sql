{% snapshot employee_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

SELECT *
FROM {{ ref('stg_employees') }}

{% endsnapshot %}

/*
This tracks employee changes like:

salary
department
role
performance_rating
*/