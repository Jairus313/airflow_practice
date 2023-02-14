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
    dag_id = "first_dag",
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

    # task instance.
    task1