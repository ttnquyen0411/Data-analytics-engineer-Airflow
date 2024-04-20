import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def determine_next_step_based_on_dimension_table_exist_func(ds, **kwargs):
    table_id = "bigquery_dimension_build_example.product_dimension_type_2"
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    is_table_exists = True
    try:
        table = bigquery_client.get_table(table_id)
    except:
        is_table_exists = False

    if is_table_exists:
        return "load_new_data_into_product_dimension_table"

    if not is_table_exists:
        return "create_new_product_dimension_table"


def create_new_product_dimension_table_func(ds, **kwargs):
    sql = f"""
        CREATE OR REPLACE TABLE `bigquery_dimension_build_example.product_dimension_type_2`
        AS
            SELECT id, brand, department, category, updated_at
            FROM `bigquery_change_data_capture_example.products_delta`
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    bigquery_client.query(sql).result()


def load_new_data_into_product_dimension_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    sql = f"""
        SELECT id, brand, department, category, updated_at
        FROM `bigquery_change_data_capture_example.products_delta`
        WHERE DATE(updated_at) = '{execution_date.strftime("%Y-%m-%d")}'
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    table_id = "bigquery_dimension_build_example.product_dimension_type_2"

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True,
        destination=table_id
    )
    bigquery_client.query(sql, job_config=job_config).result()

with DAG(
    dag_id="qt_product_dimension_type_2_etl",
    schedule_interval="0 1 * * *",
    start_date=pendulum.DateTime(2023,1,6),
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

    load_new_data_into_product_dimension_table = PythonOperator(
        task_id="load_new_data_into_product_dimension_table",
        python_callable=load_new_data_into_product_dimension_table_func
    )

    determine_next_step_based_on_dimension_table_exist.set_downstream(
        [create_new_product_dimension_table, load_new_data_into_product_dimension_table]
    )
