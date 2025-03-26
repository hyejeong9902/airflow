import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import os
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator 

from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

db_connect = psycopg2.connect(
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT
)

# t1
def print_text(text):
    print(text)

# t2: 원천데이터 사용 학습용 데이터 구축 및 db 업데이트
def insert_data(db_connect):
    # wq 서버 db 주소
    db_connect_raw_input = psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432"
    )

    # (새로 저장할) 원천데이터 불러오기(SQL사용) / test할 때 limit 걸어서 불러와지는지 확인하기! / 2025-03-25인  데이터만 불러오기
    with db_connect_raw_input.cursor() as cur:
        AAA260MT = pd.read_sql_query('SELECT * FROM AAA260MT LIMIT 5', db_connect_raw_input)
        AAA010MT = pd.read_sql_query('SELECT * FROM AAA010MT LIMIT 5', db_connect_raw_input)
        AAA050DT = pd.read_sql_query('SELECT * FROM AAA050DT LIMIT 5', db_connect_raw_input)
        AAA230DT = pd.read_sql_query('SELECT * FROM AAA230DT LIMIT 5', db_connect_raw_input)
        AAA460MT = pd.read_sql_query('SELECT * FROM AAA460MT LIMIT 5', db_connect_raw_input)
        SURGERY = pd.read_sql_query('SELECT * FROM SURGERY LIMIT 5', db_connect_raw_input)
        EXAM = pd.read_sql_query('SELECT * FROM EXAM LIMIT 5', db_connect_raw_input)
        BOJOGI = pd.read_sql_query('SELECT * FROM BOJOGI LIMIT 5', db_connect_raw_input)
        BCA201DT = pd.read_sql_query('SELECT * FROM BCA201DT LIMIT 5', db_connect_raw_input)
        BCA200MT = pd.read_sql_query('SELECT * FROM BCA200MT LIMIT 5', db_connect_raw_input)


###############################################################################3

# timezone 한국시간으로 변경
kst = pendulum.timezone("Asia/Seoul")
# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}

with DAG(
    dag_id="dag_train",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
    description='train model',
    #schedule="30 6 * * *",
    schedule_interval=None,
    catchup=False,
    tags=['train']
) as dag:
    
    t1 = PythonOperator(
        task_id="strt_job",
        python_callable=print_text,
        op_args={"text":"start train model"}
    )

    t2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
        op_args={"db_connect":db_connect}
    )

    t3 = PythonOperator(
        task_id="end_job",
        python_callable=print_text,
        op_args={"text":"end train model"}
    )

    t1 >> t2 >> t3


















