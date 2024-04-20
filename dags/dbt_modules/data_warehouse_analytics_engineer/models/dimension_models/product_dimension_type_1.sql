{{
    config(
        materialized='incremental',
        unique_key=["id"],
        incremental_strategy="merge",
        partition_by={
            'field': 'updated_at',
            'data_type': 'TIMESTAMP',
            'granularity': 'DAY'
        }
    )
}}

SELECT id, brand, department, category, updated_at
FROM {{ source("bigquery_change_data_capture_example", "products_main") }}
WHERE
{% if var('execution_date','not_set') != 'not_set' %}
    DATE(updated_at) = '{{ var('execution_date') }}'
{% elif is_incremental() %}
    DATE(updated_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))
{% else %}
    DATE(updated_at) >= "2022-01-01"
{% endif %}
