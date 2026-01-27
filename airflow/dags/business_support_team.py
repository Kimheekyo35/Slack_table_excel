from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import Slackwebhook

def start():
    print("start")

default_args = {
    'owner':'Airflow',
    'depends_on_past':False,
    'on_failure_callback':Slackwebhook.airflow_failed_callback,
    'on_success_callback':Slackwebhook.airflow_success_message,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id="Bussiness_Support_Team",
    default_args=default_args,
    start_date=datetime(2026,1,28),
    schedule="0 1 * * *",
    catchup=False,
    tags=["Bussiness_Support"],

) as dag:

    start_dag = PythonOperator(
        task_id = 'start_alarm',
        python_callable = start,
        on_success_callback = None
    )

    task_1 = BashOperator(
        task_id = 'collect_to_email',
        bash_command = 'python3 /opt/airflow/app/business_support_team_wema/10_daily_check_sheet_to_email.py'
    )

    start_dag >> task_1