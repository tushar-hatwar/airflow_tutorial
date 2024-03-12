import airflow
import requests
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

IP_GEOLOCATION_APIS = {
    'ip-api': 'http://ip-api.com/json/',
    'ipstack': 'https://api.ipstack.com/',
    'ipinfo': 'https://ipinfo.io/json'
}

# Try to get the country_code field from each API
# If given, the API is returned and the next task corresponding
# to this API will be executed
def check_api():
    apis = []
    for api, link in IP_GEOLOCATION_APIS.items():
        r = requests.get(link)
        try:
            data = r.json()
            if data and 'country' in data and len(data['country']):
                apis.append(api)
        except ValueError:
            pass
    return apis if len(apis) >0 else 'none'

# intentionally failing ipinfo to check which gets executed after branch python operator
def return_api_status(task_id):
    print(f"API Task ID: {task_id}")
    if task_id == 'ipinfo':
        raise Exception(f"Task {task_id} failed intentionally.")    

with DAG(dag_id='branch_dag', 
    default_args=default_args, 
    schedule_interval="@once") as dag:

    # BranchPythonOperator used to determine which function to run after its execution
    # subsequet task's failing/passing depends on their logic
    # The next task depends on the returned values from the python callable 
    # so if ip-api and ipinfo are returned then they will be started 
    # python function check_api
    check_api = BranchPythonOperator(
        task_id='check_api',
        python_callable=check_api
    )

    none = DummyOperator(
        task_id='none'
    )

    save = DummyOperator(task_id='save', trigger_rule = 'one_success')

    check_api >> none >> save #this is one independent flow

   # Dynamically create tasks according to the APIs
    for api in IP_GEOLOCATION_APIS:
        process = PythonOperator(
            task_id=api,
            python_callable=return_api_status,
            op_kwargs={'task_id': api}
        )
    
        check_api >> process >> save #this is second independent flow