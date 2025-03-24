from airflow import DAG 
import pendulum
from airflow.decorators import task

with DAG(
    dag_id = "dag_python_task_operator",
    schedule="0 2 * * 1", # 매일 6시 30분 마다 수행
    start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),  
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context("task_decorator 실행")