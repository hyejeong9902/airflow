from airflow import DAG 
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp # import 하는 경로 작성

with DAG(
    dag_id = "dag_python_import_func",
    schedule="30 6 * * *", # 매일 6시 30분 마다 수행
    start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable=get_sftp # 이 함수를 돌리겠다는 의미
    )