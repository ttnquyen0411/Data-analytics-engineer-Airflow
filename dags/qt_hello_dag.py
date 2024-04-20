import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def example_operator_1_func(ds, **kwargs):
    name = kwargs["name"]
    print(f"Hello! My_name is {name}")

    task_instance = kwargs["task_instance"]
    task_instance.xcom_push(key="NAME", value=name)


def example_operator_2_func(ds, **kwargs):
    age = kwargs["age"]

    task_instance = kwargs["task_instance"]
    name = task_instance.xcom_pull(task_ids="operator_1", key="NAME")
    name_1 = task_instance.xcom_pull(key="NAME")

    print(f"Name pulled using task_id: {name}")
    print(f"Name pulled without using task_id: {name_1}")
#
#     task_instance = kwargs["task_instance"]
#     name = task_instance.xcom_pull(task_ids="operator_1", key="NAME")
#     # Because currently the key "NAME" is unique,
#     # So the following code return the same value
#     # name = task_instance.xcom_pull(key="NAME")
#
#     print(f"Hello! I'm {name}. I'm {age} years old")


with DAG(
    dag_id="qt_hello_dag",
    schedule_interval=None,
    start_date=pendulum.DateTime(2023,2,12),
) as dag:

    example_operator_1 = PythonOperator(
        task_id="operator_1",
        python_callable=example_operator_1_func,
        op_kwargs={
            "name": "Huy"
        },
        dag=dag
    )

    example_operator_2 = PythonOperator(
        task_id="operator_2",
        python_callable=example_operator_2_func,
        op_kwargs={
            "age": 32
        },
        dag=dag
    )

    # example_operator_1.set_downstream(example_operator_2)
    # equally to
    example_operator_1 >> example_operator_2