

   

      
      

    merge into `data-analytics-engineer`.`bigquery_example_dbt`.`product_dimension` as DBT_INTERNAL_DEST
        using (
          
    






SELECT id, brand, department, category, updated_at
FROM (
    SELECT id, brand, department, category, updated_at,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_num
    FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`products_delta`
    WHERE DATE(updated_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))
)
WHERE row_num = 1
        ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and TIMESTAMP_trunc(DBT_INTERNAL_DEST.updated_at, DAY) in (
              TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY)
          ) 
        then delete

    when not matched then insert
        (`id`, `brand`, `department`, `category`, `updated_at`)
    values
        (`id`, `brand`, `department`, `category`, `updated_at`)



  


    