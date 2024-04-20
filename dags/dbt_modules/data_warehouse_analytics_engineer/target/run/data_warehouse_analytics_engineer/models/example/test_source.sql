

  create or replace view `data-analytics-engineer`.`bigquery_example_dbt`.`test_source`
  OPTIONS()
  as SELECT *
FROM `data-analytics-engineer`.`bigquery_change_data_capture_example`.`order_items_delta`;

