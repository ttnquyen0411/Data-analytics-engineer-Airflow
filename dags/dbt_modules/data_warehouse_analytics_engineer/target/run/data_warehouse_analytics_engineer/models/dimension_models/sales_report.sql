

   

      
      

    merge into `data-analytics-engineer`.`bigquery_example_dbt`.`sales_report` as DBT_INTERNAL_DEST
        using (
          
    






WITH sales_transaction_data AS (
    SELECT *
    FROM `data-analytics-engineer`.`bigquery_example_dbt`.`sales_transaction_fact` sales_trans
    WHERE DATE(created_at) = DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
SELECT DATE(created_at) AS transaction_date, category, brand, department, SUM(sale_price) AS total_sales
FROM (
 SELECT * EXCEPT(row_num)
 FROM (
   SELECT created_at, category, brand, department, sale_price,
   ROW_NUMBER() OVER(PARTITION BY sales_trans.id ORDER BY product_dim.updated_at DESC) AS row_num
   FROM sales_transaction_data sales_trans
     LEFT JOIN `data-analytics-engineer`.`bigquery_example_dbt`.`product_dimension` product_dim
       ON sales_trans.product_id = product_dim.id and sales_trans.created_at >= product_dim.updated_at
 )
 WHERE row_num = 1
)
GROUP BY transaction_date, category, brand, department
        ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and DBT_INTERNAL_DEST.transaction_date in (
              DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          ) 
        then delete

    when not matched then insert
        (`transaction_date`, `category`, `brand`, `department`, `total_sales`)
    values
        (`transaction_date`, `category`, `brand`, `department`, `total_sales`)



  


    