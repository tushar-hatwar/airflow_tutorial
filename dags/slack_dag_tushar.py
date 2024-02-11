from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.providers.slack.operators.slack import SlackAPIFileOperator
import time

SLACK_CONNECTION_ID = "slack_conn"
SLACK_CHANNEL = "alerts"
SLACK_MESSAGE = """
Hello! The {{ ti.task_id }} task is saying hi :wave: 
Today is the {{ ds }}, time is {{ts}} and this task finished with the state: {{ ti.state }} :tada:.
"""

@dag(
    start_date=datetime(2023, 4, 18),
    schedule=None,
    catchup=False,
    tags=["Notifier", "Slack"],
)
def notifier_slack():
    @task(
        on_success_callback=SlackNotifier(
            slack_conn_id=SLACK_CONNECTION_ID,
            text=SLACK_MESSAGE,
            channel=SLACK_CHANNEL,
        ),
    )
    def post_to_slack1():
        return 10

    @task
    def post_to_slack2(**kwargs):
        ti = kwargs['ti']
        pulled_message = ti.xcom_pull(task_ids='post_to_slack1')
        print(pulled_message)
        return pulled_message
    
    @task(
        on_success_callback = SlackAPIFileOperator(
        task_id="post_to_slack2",
        slack_conn_id=SLACK_CONNECTION_ID,
        channel=SLACK_CHANNEL,
        initial_comment="slack_dag_tushar.py",
        filename="/opt/airflow/dags/slack_dag_tushar.py",
        filetype="txt",
        ),
    )
    def slack_operator_file():
        return 10
    
    task1 = post_to_slack1()
    task2 = post_to_slack2()
    task3 = slack_operator_file()

    [task1 , task2] >> task3

notifier_slack()
