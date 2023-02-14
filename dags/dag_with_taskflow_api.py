from airflow.decorators import dag, task

from datetime import datetime, timedelta



default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


@dag(dag_id = "dag_with_taskflow_api",
    default_args = default_args,
    start_date = datetime(2023, 2, 1),
    schedule_interval = "@daily")
def taskflow_etl():

    @task(multiple_outputs = True)
    def get_name():
        return {
            "first_name": "Geralt",
            "last_name": "Rivia"
        }

    @task()
    def get_age():
        return 101

    @task()
    def greet(firstname, lastname, age):
        print("Hello World, I am {} {} and I am {} old".format(firstname, lastname, age))

    name_dict = get_name()
    age = get_age()

    greet(firstname=name_dict["first_name"],
        lastname=name_dict["last_name"],
        age=age)

greet_dag = taskflow_etl()