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
            'field': 'created_at',
            'data_type': 'TIMESTAMP',
            'granularity': 'DAY'
        },
        partitions = partitions_to_replace
    )
}}

SELECT
    id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC) AS row_num
    FROM {{ source("bigquery_change_data_capture_example", "order_items_delta") }}
    WHERE DATE(created_at) = DATE({{ execution_date }})
)
WHERE row_num = 1
