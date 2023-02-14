from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta



# default args for DAG run.
default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

# creating the DAG.
with DAG(
    dag_id = "first_dag_v5",
    default_args = default_args,
    description = "first dag w/ bash operator",
    start_date = datetime(2022, 2, 1, 2),
    schedule_interval = "@daily"
) as dag:
    # creating bash task.
    task1 = BashOperator(
        task_id = "first_task",
        bash_command = "echo hello world!"
    )

    task2 = BashOperator(
        task_id = "second_task",
        bash_command = "echo hello world2!"
    )

    task3 = BashOperator(
        task_id = "third_task",
        bash_command = "echo hello world3!"
    )

    # task instance.
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # with bit shift operator
    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3]