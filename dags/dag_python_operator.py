from airflow import DAG 
import pendulum
import datetime
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id = "dag_python_operator",
    schedule="30 6 * * *", # 매일 6시 30분 마다 수행
    start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
    

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit # 이 함수를 돌리겠다는 의미
    )

    py_t1