
    






SELECT id, brand, department, category, updated_at
FROM (
    SELECT id, brand, department, category, updated_at,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_num
    FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`products_delta`
    WHERE DATE(updated_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))
)
WHERE row_num = 1