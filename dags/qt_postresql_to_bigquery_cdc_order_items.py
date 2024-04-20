import datetime
import json
from tempfile import NamedTemporaryFile

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage, bigquery

import os

import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq

# Step 1: Make cursor to query data
from google.oauth2.service_account import Credentials
from google.cloud import storage


def extract_postgresql_to_gcs_func(ds, **kwargs):
    postgres_connection = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        user="postgres",
        password="992907",
        database="data-analytics-engineer"
    )

    postgres_cursor = postgres_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    end_date = execution_date.replace(minute=0, second=0, microsecond=0)
    start_time = end_date - datetime.timedelta(hours=1)

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    upload_file_prefix = start_time.strftime("%Y-%m-%d") + "/" + start_time.strftime("%H:%M:%S")

    sql = f"""
    SELECT * FROM order_items WHERE created_at >= '{start_time_str} UTC' AND created_at < '{end_time_str} UTC'
    """
    print(sql)
    postgres_cursor.execute(sql)

    from_postgres_to_bq_type_map = {
        1114: "DATETIME",
        1184: "TIMESTAMP",
        1082: "DATE",
        1083: "TIME",
        1005: "INTEGER",
        1007: "INTEGER",
        1016: "INTEGER",
        20: "INTEGER",
        21: "INTEGER",
        23: "INTEGER",
        16: "BOOL",
        700: "FLOAT",
        701: "FLOAT",
        1700: "FLOAT",
    }
    from_bq_to_parquet_type_map = {
        "INTEGER": pa.int64(),
        "FLOAT": pa.float64(),
        "NUMERIC": pa.float64(),
        "BIGNUMERIC": pa.float64(),
        "BOOL": pa.bool_(),
        "STRING": pa.string(),
        "BYTES": pa.binary(),
        "DATE": pa.date32(),
        "DATETIME": pa.date64(),
        "TIMESTAMP": pa.timestamp("s"),
    }
    column_names = [field[0] for field in postgres_cursor.description]
    bq_fields = []
    for field in postgres_cursor.description:
        bq_fields.append({
            "name": field[0],
            "type": from_postgres_to_bq_type_map.get(field[1], "STRING"),
            "mode": "REPEATED" if field[1] in (1009, 1005, 1007, 1016) else "NULLABLE",

        })

    bq_types = [bq_field.get("type") if bq_field is not None else None for bq_field in bq_fields]
    pq_types = [from_bq_to_parquet_type_map.get(bq_type, pa.string()) for bq_type in bq_types]
    parquet_schema = pa.schema(zip(column_names, pq_types))

    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)
    storage_client = storage.Client(credentials=credentials)

    file_no = 1
    tmp_file = NamedTemporaryFile(delete=True)
    bucket_name = "data-analytics-engineer-example-quyentran"
    bucket = storage_client.bucket(bucket_name)
    parquet_writer = pq.ParquetWriter(tmp_file.name, parquet_schema)

    count = 0
    for row in postgres_cursor:
        dict_row = dict(row)
        pq_dict_row = {key: [dict_row[key]] for key in dict_row}
        parquet_tbl = pa.Table.from_pydict(pq_dict_row, parquet_schema)
        parquet_writer.write_table(parquet_tbl)

        count = count + 1

        if count >= 1000:
            parquet_writer.close()
            tmp_file.flush()
            # Upload to GCS
            print(f"start uploading {tmp_file.name} to {file_no}.parquet")
            upload_filename = f"data/order_items/{upload_file_prefix}/{file_no}.parquet"  # the name of blob
            blob = bucket.blob(upload_filename)
            blob.upload_from_filename(tmp_file.name)
            print(f"finish uploading {tmp_file.name} to {file_no}.parquet - Remove file {tmp_file.name}")
            tmp_file.close()


            file_no += 1
            tmp_file = NamedTemporaryFile(delete=True)
            parquet_writer = pq.ParquetWriter(tmp_file.name, parquet_schema)

            count = 0

    parquet_writer.close()
    tmp_file.flush()
    upload_filename = f"data/order_items/{upload_file_prefix}/{file_no}.parquet"  # the name of blob
    blob = bucket.blob(upload_filename)
    blob.upload_from_filename(tmp_file.name)
    tmp_file.close()

    postgres_cursor.close()
    postgres_connection.close()

    # # Adding schema
    # schema_text = json.dumps(bq_fields, sort_keys=True)
    # tmp_schema_file_handle = NamedTemporaryFile(delete=True)
    # tmp_schema_file_handle.write(schema_text.encode("utf-8"))
    # tmp_schema_file_handle.flush()
    # schema_upload_filename = f"data/order_items/{upload_file_prefix}/schema.json"  # the name of blob
    # blob = bucket.blob(schema_upload_filename)
    # blob.upload_from_filename(tmp_schema_file_handle.name)
    # tmp_schema_file_handle.close()


def load_extracted_file_to_bigquery_func(ds, **kwargs):
    service_account_file = "/opt/airflow/credentials/data-analytics-engineer-d7e247899ebd.json"
    credentials = Credentials.from_service_account_file(service_account_file)

    job_config = bigquery.LoadJobConfig(
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
    )

    table_id = f"bigquery_change_data_capture_example.order_items_delta"
    dag_run = kwargs["dag_run"]
    execution_date = dag_run.execution_date
    end_date = execution_date.replace(minute=0, second=0, microsecond=0)
    start_time = end_date - datetime.timedelta(hours=1)

    upload_file_prefix = start_time.strftime("%Y-%m-%d") + "/" + start_time.strftime("%H:%M:%S")

    bucket_name = "data-analytics-engineer-example-quyentran"

    bigquery_client = bigquery.Client(credentials=credentials)
    job = bigquery_client.load_table_from_uri(
        source_uris=f"gs://{bucket_name}/data/order_items/{upload_file_prefix}/*.parquet",
        destination=table_id,
        job_config=job_config
    )
    job.result()


with DAG(
    dag_id="qt_postgresql_to_bigquery_cdc_order_items",
    schedule_interval="15 * * * *",
    start_date=pendulum.DateTime(2023,1,7),
    catchup=False
) as dag:

    extract_postgresql_to_gcs = PythonOperator(
        task_id="extract_postgresql_to_gcs_task",
        python_callable=extract_postgresql_to_gcs_func
    )

    load_extracted_file_to_bigquery = PythonOperator(
        task_id="load_extracted_file_to_bigquery_delta_table",
        python_callable=load_extracted_file_to_bigquery_func
    )

    extract_postgresql_to_gcs.set_downstream(load_extracted_file_to_bigquery)