import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

# Define a list of URLs to check
URLS_TO_CHECK = {
    'Google': 'https://www.google.com',
    'Facebook': 'https://www.facebook.com',
    'Twitter': 'https://www.twitter.com'
}

# Function to check if a URL is accessible
def check_url():
    for name, url in URLS_TO_CHECK.items():
        try:
            response = requests.get(url)
            if response.status_code == 200 and name == 'Facebook':
                return name
        except Exception as e:
            pass
    return 'none'

with DAG(dag_id='branch_check_dag', 
    default_args=default_args, 
    schedule_interval="@once") as dag:

    # BranchPythonOperator
    check_url_task = BranchPythonOperator(
        task_id='check_url',
        python_callable=check_url
    )

    # Dummy task for when no URL is accessible
    none_task = DummyOperator(
        task_id='none_task'
    )

    # Dummy task for saving results
    save_task = DummyOperator(task_id='save_task', trigger_rule='one_success')

    check_url_task >> none_task >> save_task

    # Dynamically create tasks according to the URLs
    for name, url in URLS_TO_CHECK.items():
        process_task = DummyOperator(
            task_id=name
        )
    
        check_url_task >> process_task >> save_task
