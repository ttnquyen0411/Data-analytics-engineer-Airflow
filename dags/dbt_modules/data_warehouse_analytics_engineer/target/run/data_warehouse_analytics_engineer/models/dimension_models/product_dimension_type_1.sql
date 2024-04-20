
        
            
                
                
            
        
    

    

    merge into `data-analytics-engineer`.`bigquery_example_dbt`.`product_dimension_type_1` as DBT_INTERNAL_DEST
        using (
          

SELECT id, brand, department, category, updated_at
FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`products_main`
WHERE

    DATE(updated_at) = DATE(TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY))

        ) as DBT_INTERNAL_SOURCE
        on 
                    DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
                

    
    when matched then update set
        `id` = DBT_INTERNAL_SOURCE.`id`,`brand` = DBT_INTERNAL_SOURCE.`brand`,`department` = DBT_INTERNAL_SOURCE.`department`,`category` = DBT_INTERNAL_SOURCE.`category`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`
    

    when not matched then insert
        (`id`, `brand`, `department`, `category`, `updated_at`)
    values
        (`id`, `brand`, `department`, `category`, `updated_at`)


    