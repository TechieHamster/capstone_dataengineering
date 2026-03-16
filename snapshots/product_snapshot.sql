{% snapshot product_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='last_modified_date'
    )
}}

SELECT *
FROM {{ ref('stg_products') }}

{% endsnapshot %}

/*This tracks product changes such as:

price
stock
brand
category*/