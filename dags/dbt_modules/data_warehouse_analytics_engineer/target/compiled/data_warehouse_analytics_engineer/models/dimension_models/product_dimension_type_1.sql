

SELECT id, brand, department, category, updated_at
FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`products_main`
WHERE

    DATE(updated_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))
