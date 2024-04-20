import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum
from google.oauth2.service_account import Credentials


def execute_merge_data_from_delta_to_main_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date

    table_name = kwargs["table_name"]
    # Write the code to generate sql base on table_name

    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    main_table = bigquery_client.get_table("bigquery_change_data_capture_example.products_main")
    main_table_schema = main_table.schema
    main_table_schema_field_names = []
    for field in main_table_schema:
        main_table_schema_field_names.append(field.name)

    insert_express = ",".join(main_table_schema_field_names)
    insert_value_list = []
    for field_name in main_table_schema_field_names:
        insert_value_list.append(f"d.{field_name}")
    insert_value_express = ",".join(insert_value_list)

    set_update_list = []
    for field_name in main_table_schema_field_names:
        if field_name == "id":
            continue
        set_update_list.append(f"{field_name} = d.{field_name}")
    set_update_express = ",".join(set_update_list)

    sql = f"""
        MERGE bigquery_change_data_capture_example.products_main m
        USING
          (
          SELECT * EXCEPT(row_num)
          FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS row_num
            FROM bigquery_change_data_capture_example.products_delta delta
            WHERE DATE(updated_at) = '{execution_date.strftime("%Y-%m-%d")}'
            )
          WHERE row_num = 1
        ) d
        ON  m.id = d.id

        WHEN NOT MATCHED THEN
        INSERT ({insert_express})
        VALUES ({insert_value_express})

        WHEN MATCHED AND (m.updated_at < d.updated_at) THEN
        UPDATE
        SET {set_update_express}
    """
    print(sql)

    # bigquery_client.query(sql).result()


with DAG(
        dag_id="merge_from_delta_table_to_main_table",
        schedule_interval="30 0 * * *",
        start_date=pendulum.DateTime(2023, 2, 11),
        catchup=False
) as dag:
    execute_merge_data_from_delta_to_main_table = PythonOperator(
        task_id="execute_merge_data_from_delta_to_main_table",
        python_callable=execute_merge_data_from_delta_to_main_table_func,
        op_kwargs={
            "table_name": "products"
        }
    )
