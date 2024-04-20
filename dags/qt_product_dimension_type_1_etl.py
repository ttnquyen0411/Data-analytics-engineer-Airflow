import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def determine_next_step_based_on_dimension_table_exist_func(ds, **kwargs):
    table_id = "bigquery_dimension_build_example.product_dimension"
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    is_table_exists = True
    try:
        table = bigquery_client.get_table(table_id)
    except:
        is_table_exists = False

    if is_table_exists:
        return "merge_new_data_into_product_dimension_table"

    return "create_new_product_dimension_table"


def create_new_product_dimension_table_func(ds, **kwargs):
    sql = f"""
        CREATE OR REPLACE TABLE `bigquery_dimension_build_example.product_dimension`
        AS
            SELECT id, brand, department, category, updated_at
            FROM `bigquery_change_data_capture_example.products_main`
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    bigquery_client.query(sql).result()


def merge_new_data_into_product_dimension_table_func(ds, **kwargs):
    table_id = "bigquery_dimension_build_example.product_dimension"
    sql = f"""
        MERGE {table_id} m
        USING (
            SELECT id, brand, department, category, updated_at
            FROM `bigquery_change_data_capture_example.products_main`
            WHERE DATE(updated_at) = CURRENT_DATE()
        ) d
        ON m.id = d.id
        WHEN NOT MATCHED THEN
        INSERT (id, brand, department, category, updated_at)
        VALUES (d.id, d.brand, d.department, d.category, d.updated_at)

        WHEN MATCHED AND (m.updated_at < d.updated_at) THEN
        UPDATE
        SET brand = d.brand, department = d.department, category = d.category, updated_at = d.updated_at
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    bigquery_client.query(sql).result()


with DAG(
        dag_id="qt_product_dimension_type_1_etl",
        schedule_interval="0 1 * * *",
        start_date=pendulum.DateTime(2023, 1, 6),
        catchup=False
) as dag:
    determine_next_step_based_on_dimension_table_exist = BranchPythonOperator(
        task_id="determine_next_step_based_on_dimension_table_exist",
        python_callable=determine_next_step_based_on_dimension_table_exist_func
    )

    create_new_product_dimension_table = PythonOperator(
        task_id="create_new_product_dimension_table",
        python_callable=create_new_product_dimension_table_func
    )

    merge_new_data_into_product_dimension_table = PythonOperator(
        task_id="merge_new_data_into_product_dimension_table",
        python_callable=merge_new_data_into_product_dimension_table_func
    )

    determine_next_step_based_on_dimension_table_exist.set_downstream(
        [create_new_product_dimension_table, merge_new_data_into_product_dimension_table]
    )
