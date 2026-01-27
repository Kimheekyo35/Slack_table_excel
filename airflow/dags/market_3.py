from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import Slackwebhook

def start():
    print ("start")

def end():
    print("end")

default_args={
    'owner':'Airflow',
    'depends_on_past':False,
    # 실패/성공 시 슬랙으로 알람 보내기
    'on_failure_callback':Slackwebhook.airflow_failed_callback,
    'on_success_callback':Slackwebhook.airflow_success_message,
}

with DAG(
    dag_id = "marketing_3th_daily_crawling",
    default_args=default_args,
    start_date = datetime(2026, 1, 27),
    schedule = "0 21 * * *",   # Airflow 3.x 방식
    catchup = False,
    tags=["marketing_3th"],
    # ✅ DAG 성공/실패 1번만
    
) as dag:

    # default_args 해두면 여기에서 파라미터 설정 가능
    start_dag = PythonOperator(
        task_id = 'start_alarm',
        python_callable = start,
        on_success_callback = None
    )

    task_1 = BashOperator(
    task_id = 'x_cralwing',
    bash_command = 'python3 /home/linuxuser/wemarketing/airflow_test/market_3th_team/07_daily_3team_x.py'
    )

    task_2 = BashOperator(
        task_id = 'insta_crawling',
        bash_command = 'python3 /home/linuxuser/wemarketing/airflow_test/market_3th_team/07_daily_3team_insta.py'
    )

    task_3 =  BashOperator(
        task_id = 'tiktok_crawling',
        bash_command = 'python3 /home/linuxuser/wemarketing/airflow_test/market_3th_team/07_daily_3team_tiktok.py'
    )



    start_dag >> task_1
    start_dag >> task_2
    start_dag >> task_3