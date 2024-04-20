{% if var('execution_date','not_set') != 'not_set' %}
    {% set execution_date = '"' + var('execution_date') + '"' %}
{% else %}
    {% set execution_date = "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)" %}
{% endif %}

{% set partitions_to_replace = [
    execution_date
] %}

{{
    config(
        materialized='incremental',
        incremental_strategy="insert_overwrite",
        partition_by={
            'field': 'transaction_date',
            'data_type': 'DATE',
            'granularity': 'DAY'
        },
        partitions = partitions_to_replace
    )
}}


SELECT
  DATE(created_at) AS transaction_date,
  SUM(IF(returned_at IS NULL, sale_price, sale_price * -1)) AS total_sale
FROM {{ ref("sales_transaction_fact") }}
WHERE DATE(created_at) = DATE({{ execution_date }})
GROUP BY 1
