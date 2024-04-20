SELECT *
FROM {{ source("bigquery_change_data_capture_example", "order_items_delta") }}