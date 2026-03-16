--text standardize

{% macro clean_text(column) %}
    INITCAP(TRIM({{ column }}))
{% endmacro %}

--lowercase email

{%macro clean_email(column) %}
    LOWER(TRIM({{ column }}))
{% endmacro %}

--standardize Dates
{% macro parse_date(column) %}
    COALESCE(
        TRY_TO_DATE({{ column }}, 'YYYY-MM-DD'),
        TRY_TO_DATE({{ column }}, 'DD-MM-YYYY'),
        TRY_TO_DATE({{ column }})
    )
{% endmacro %}

--parse timestamp

{% macro parse_timestamp(column) %}

COALESCE(
    TRY_TO_TIMESTAMP({{ column }}),
    TRY_TO_TIMESTAMP({{ column }}, 'YYYY-MM-DD HH24:MI:SS'),
    TRY_TO_TIMESTAMP({{ column }}, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
)

{% endmacro %}

--Currency handling
{% macro clean_currency(column) %}
    TRY_TO_NUMBER(
        REGEXP_REPLACE({{ column }}, '[^0-9.]','')
    )
{% endmacro %}

--Remove Special Characters
{% macro remove_special_chars(column) %}
    REGEXP_REPLACE({{ column }}, '[^a-zA-Z0-9]','')
{% endmacro %}

--Handling missing and null values
{% macro default_if_null(colummn,default_value) %}
    COALESCE({{ column }},{{ default_value }})
{% endmacro%}

--mask phone_number

{%  macro mask_phone(column) %}
    CASE 
        WHEN {{ column }} IS NULL THEN NULL
        ELSE 
            CONCAT(
                LEFT(REGEXP_REPLACE({{ column }}, '[^0-9X]',''),
                    LENGTH(REGEXP_REPLACE({{ column }}, '[^0-9]','')) - 4),
                    'XXXX'
            ) 
    END
{% endmacro %}