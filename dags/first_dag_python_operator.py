from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta



# default args for DAG run.
default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

# basic function
def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="firstname")
    last_name = ti.xcom_pull(task_ids="get_name", key="lastname")

    age = ti.xcom_pull(task_ids="get_age", key="age")

    print("Hello World, My name is {} {} and I am {} old".format(first_name, last_name, age))

def get_username(ti):
    ti.xcom_push(key="firstname", value="Geralt")
    ti.xcom_push(key="lastname", value="Rivia")

def get_age(ti):
    ti.xcom_push(key="age", value=101)


# creating the DAG.
with DAG(
    dag_id = "first_dag_python_operator_v6",
    default_args = default_args,
    description = "first dag w/ python operator",
    start_date = datetime(2022, 2, 1),
    schedule_interval = "@daily"
) as dag:
    # task with python operator.
    task1 = PythonOperator(
        task_id = "greet",
        python_callable = greet
    )

    task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_username
    )

    task3 = PythonOperator(
        task_id = "get_age",
        python_callable = get_age
    )

    [task2, task3] >> task1