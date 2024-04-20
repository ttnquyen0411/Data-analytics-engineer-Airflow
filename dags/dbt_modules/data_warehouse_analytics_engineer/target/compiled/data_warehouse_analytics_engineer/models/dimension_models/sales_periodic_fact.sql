
    







SELECT
  DATE(created_at) AS transaction_date,
  SUM(IF(returned_at IS NULL, sale_price, sale_price * -1)) AS total_sale
FROM `data-analytics-engineer`.`bigquery_example_dbt`.`sales_transaction_fact`
WHERE DATE(created_at) = DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
GROUP BY 1