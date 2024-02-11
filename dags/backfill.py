from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 31),
    'owner': 'Airflow',
    'email': 'owner@test.com',
    'retries':2 ,
    'retry_delay': timedelta(seconds = 20)
}

with DAG(dag_id='backfill', schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
    
    # Task 1
    bash_task_1 = BashOperator(task_id='bash_task_1', bash_command="echo 'first task'")
    
    # Task 2
    bash_task_2 = BashOperator(task_id='bash_task_2', bash_command="echo 'second task'")

    bash_task_1 >> bash_task_2