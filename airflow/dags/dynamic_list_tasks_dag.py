from datetime import datetime
from random import randint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

def generate_random_numbers():
    random_numbers = [0, 3, 5, 7, 9]
    return random_numbers

def print_number(number, **kwargs):
    print(f"Number: {number}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 4, 1),
}

dag = DAG(
    dag_id="dynamic_list_tasks_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

with dag:
    random_numbers = generate_random_numbers()

    with TaskGroup(group_id="print_numbers_group") as print_numbers_group:
        for number in random_numbers:
            task_id = f"print_number_{number}"
            PythonOperator(
                task_id=task_id,
                python_callable=print_number,
                op_args=[number],
                dag=dag,
            )
