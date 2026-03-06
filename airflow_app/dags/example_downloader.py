
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.standard.operators.bash import BashOperator


def print_hello(name):
    print(f'hello world {name}')

with DAG(
    dag_id="usda_downloader",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * 1",
    catchup=False,
    tags=["downloader", "usda"],
) as dag:
    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
        op_kwargs={"name": 'Satyaki'},
        # trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    task2 = PythonOperator(
        task_id="print_hello_2",
        python_callable=print_hello,
        op_kwargs={"name": 'Kristiana'},
    )

    taskbash = BashOperator(
        task_id="my_name",
        bash_command="echo https://airflow.apache.org/",
    )


    # task1 >> task2 >> taskbash

    [task2, taskbash] >> task1

