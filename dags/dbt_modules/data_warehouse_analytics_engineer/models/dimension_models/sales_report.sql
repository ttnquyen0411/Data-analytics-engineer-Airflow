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

WITH sales_transaction_data AS (
    SELECT *
    FROM {{ ref("sales_transaction_fact") }} sales_trans
    WHERE DATE(created_at) = DATE({{ execution_date }})
)
SELECT DATE(created_at) AS transaction_date, category, brand, department, SUM(sale_price) AS total_sales
FROM (
 SELECT * EXCEPT(row_num)
 FROM (
   SELECT created_at, category, brand, department, sale_price,
   ROW_NUMBER() OVER(PARTITION BY sales_trans.id ORDER BY product_dim.updated_at DESC) AS row_num
   FROM sales_transaction_data sales_trans
     LEFT JOIN {{ ref("product_dimension") }} product_dim
       ON sales_trans.product_id = product_dim.id and sales_trans.created_at >= product_dim.updated_at
 )
 WHERE row_num = 1
)
GROUP BY transaction_date, category, brand, department
