

  create or replace table `data-analytics-engineer`.`bigquery_example_dbt_1`.`my_second_dbt_model_1`
  
  
  OPTIONS()
  as (
    
-- Use the `ref` function to select from other models

select *
from `data-analytics-engineer`.`bigquery_change_data_capture_example`.`order_items_delta`
LIMIT 10
  );
  