import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

DAG_NAME="test_taskgroup"

default_args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

def factory_taskgroup(group_id):
    with TaskGroup(group_id=group_id) as tg:
        t1 = DummyOperator(task_id='task1')
        t2 = DummyOperator(task_id='task2')
    return tg

with DAG(dag_id=DAG_NAME, default_args=default_args, schedule_interval="@once") as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup("group1") as tg1:
        factory_taskgroup('subgroup1')
        factory_taskgroup('subgroup2')

    some_other_task = DummyOperator(task_id='check')

    with TaskGroup("group2") as tg2:
        factory_taskgroup('subgroup1')

    end = DummyOperator(task_id='final')

    start >> tg1 >> some_other_task >> tg2 >> end