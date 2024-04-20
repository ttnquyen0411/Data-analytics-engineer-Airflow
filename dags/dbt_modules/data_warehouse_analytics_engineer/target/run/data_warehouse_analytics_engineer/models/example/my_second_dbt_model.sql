

  create or replace view `data-analytics-engineer`.`bigquery_example_dbt`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `data-analytics-engineer`.`bigquery_example_dbt`.`my_first_dbt_model`
where id = 1;

