import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def determine_next_step_based_on_sales_transaction_fact_table_exist_func(ds, **kwargs):
    table_id = "bigquery_dimension_build_example.sales_transaction_fact"
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    is_table_exists = True
    try:
        table = bigquery_client.get_table(table_id)
    except:
        is_table_exists = False

    if is_table_exists:
        return "merge_new_data_into_sales_transaction_fact_table"

    return "create_new_sales_transaction_fact_table"


def create_new_sales_transaction_fact_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    sql = f"""
        CREATE OR REPLACE TABLE bigquery_dimension_build_example.sales_transaction_fact AS
        SELECT 
            order_id,
            user_id,
            product_id,
            inventory_item_id,
            status,
            created_at,
            shipped_at,
            delivered_at,
            returned_at,
            sale_price
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC) AS row_num
            FROM bigquery_change_data_capture_example.order_items_delta
            WHERE DATE(created_at) = '{execution_date_str}'
        )
        WHERE row_num = 1

    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    bigquery_client.query(sql).result()


def merge_new_data_into_sales_transaction_fact_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    table_id = "bigquery_dimension_build_example.sales_transaction_fact"
    delete_old_data_sql = f"""
        DELETE FROM {table_id}
        WHERE DATE(created_at) = '{execution_date_str}'
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    logging.info(f"Execute SQL: {delete_old_data_sql}")
    bigquery_client.query(delete_old_data_sql).result()

    insert_data_sql = f"""
        INSERT INTO {table_id}
        SELECT 
            order_id,
            user_id,
            product_id,
            inventory_item_id,
            status,
            created_at,
            shipped_at,
            delivered_at,
            returned_at,
            sale_price
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC) AS row_num
            FROM bigquery_change_data_capture_example.order_items_delta
            WHERE DATE(created_at) = '{execution_date_str}'
        )
        WHERE row_num = 1
    """

    logging.info(f"Execute SQL: {insert_data_sql}")
    bigquery_client.query(insert_data_sql).result()


def determine_next_step_based_on_sales_periodic_fact_table_exist_func(ds, **kwargs):
    table_id = "bigquery_dimension_build_example.sales_periodic_fact"
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    is_table_exists = True
    try:
        table = bigquery_client.get_table(table_id)
    except:
        is_table_exists = False

    if is_table_exists:
        return "merge_new_data_into_sales_periodic_fact_table"

    return "create_new_sales_periodic_fact_table"


def create_new_sales_periodic_fact_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    sql = f"""
        CREATE OR REPLACE TABLE bigquery_dimension_build_example.sales_periodic_fact AS
        SELECT 
          DATE(created_at) AS transaction_date,
          SUM(IF(returned_at IS NULL, sale_price, sale_price * -1)) AS total_sale 
        FROM `bigquery_dimension_build_example.sales_transaction_fact`
        WHERE DATE(created_at) = '{execution_date_str}'
        GROUP BY 1
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    bigquery_client.query(sql).result()


def merge_new_data_into_sales_periodic_fact_table_func(ds, **kwargs):
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    execution_date_str = execution_date.strftime("%Y-%m-%d")
    table_id = "bigquery_dimension_build_example.sales_periodic_fact"
    delete_old_data_sql = f"""
        DELETE FROM {table_id}
        WHERE transaction_date = '{execution_date_str}'
    """
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    bigquery_client = bigquery.Client(credentials=credentials)

    logging.info(f"Execute SQL: {delete_old_data_sql}")
    bigquery_client.query(delete_old_data_sql).result()

    insert_data_sql = f"""
        INSERT INTO {table_id}
        SELECT 
          DATE(created_at) AS transaction_date,
          SUM(IF(returned_at IS NULL, sale_price, sale_price * -1)) AS total_sale
        FROM `bigquery_dimension_build_example.sales_transaction_fact`
        WHERE DATE(created_at) = '{execution_date_str}'
        GROUP BY 1
    """

    logging.info(f"Execute SQL: {insert_data_sql}")
    bigquery_client.query(insert_data_sql).result()


with DAG(
        dag_id="qt_sales_fact_etl",
        schedule_interval="0 1 * * *",
        start_date=pendulum.DateTime(2023, 1, 13),
        catchup=False
) as dag:
    determine_next_step_based_on_sales_transaction_fact_table_exist = BranchPythonOperator(
        task_id="determine_next_step_based_on_sales_transaction_fact_table_exist",
        python_callable=determine_next_step_based_on_sales_transaction_fact_table_exist_func
    )

    create_new_sales_transaction_fact_table = PythonOperator(
        task_id="create_new_sales_transaction_fact_table",
        python_callable=create_new_sales_transaction_fact_table_func
    )

    merge_new_data_into_sales_transaction_fact_table = PythonOperator(
        task_id="merge_new_data_into_sales_transaction_fact_table",
        python_callable=merge_new_data_into_sales_transaction_fact_table_func
    )

    determine_next_step_based_on_sales_transaction_fact_table_exist.set_downstream(
        [create_new_sales_transaction_fact_table, merge_new_data_into_sales_transaction_fact_table]
    )

    # For sales periodic
    determine_next_step_based_on_sales_periodic_fact_table_exist = BranchPythonOperator(
        task_id="determine_next_step_based_on_sales_periodic_fact_table_exist",
        python_callable=determine_next_step_based_on_sales_periodic_fact_table_exist_func,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    create_new_sales_periodic_fact_table = PythonOperator(
        task_id="create_new_sales_periodic_fact_table",
        python_callable=create_new_sales_periodic_fact_table_func
    )

    merge_new_data_into_sales_periodic_fact_table = PythonOperator(
        task_id="merge_new_data_into_sales_periodic_fact_table",
        python_callable=merge_new_data_into_sales_periodic_fact_table_func
    )

    determine_next_step_based_on_sales_periodic_fact_table_exist.set_upstream(
        [create_new_sales_transaction_fact_table, merge_new_data_into_sales_transaction_fact_table]
    )

    determine_next_step_based_on_sales_periodic_fact_table_exist.set_downstream(
        [create_new_sales_periodic_fact_table, merge_new_data_into_sales_periodic_fact_table]
    )




