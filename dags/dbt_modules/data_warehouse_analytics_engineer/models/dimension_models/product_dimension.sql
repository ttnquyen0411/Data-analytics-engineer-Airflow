{% if var('execution_date','not_set') != 'not_set' %}
    {% set execution_date = '"' + var('execution_date') + '"' %}
{% else %}
    {% set execution_date = "TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY)" %}
{% endif %}

{% set partitions_to_replace = [
    execution_date
] %}

{{
    config(
        materialized='incremental',
        incremental_strategy="insert_overwrite",
        partition_by={
            'field': 'updated_at',
            'data_type': 'TIMESTAMP',
            'granularity': 'DAY'
        },
        partitions = partitions_to_replace
    )
}}

SELECT id, brand, department, category, updated_at
FROM (
    SELECT id, brand, department, category, updated_at,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_num
    FROM {{ source("bigquery_change_data_capture_example", "products_delta") }}
    WHERE DATE(updated_at) = DATE({{ execution_date }})
)
WHERE row_num = 1
