
    






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
    FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`order_items_delta`
    WHERE DATE(created_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))
)
WHERE row_num = 1