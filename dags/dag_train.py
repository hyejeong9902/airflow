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

# t1
def print_text(text):
    print(text)

# t2: 원천데이터 사용 학습용 데이터 구축 및 db 업데이트
db_connect = psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432"
    )

def insert_data(db_connect):

    # 학습용데이터에 추가할 데이터 불러오기(필터링 부분 수정 필요)
    with db_connect.cursor() as cur:
        # 기존 학습용데이터 테이블 4종
        TRAIN_JANGHAE_FINAL = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_FINAL"', db_connect)
        TRAIN_JANGHAE_BUWI8 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI8"', db_connect)
        TRAIN_JANGHAE_BUWI9 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI9"', db_connect)
        TRAIN_JANGHAE_BUWI10 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI10"', db_connect)
        # BCA200MT(최종장해)에서 학습용데이터에 새로 추가할 원부 추출
        # 원천 데이터 테이블에서 새로 추가할 원부 정보만 추출
        AAA260MT = pd.read_sql_query('SELECT * FROM "AAA260MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA010MT = pd.read_sql_query('SELECT * FROM "AAA010MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA050DT = pd.read_sql_query('SELECT * FROM "AAA050DT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA230DT = pd.read_sql_query('SELECT * FROM "AAA230MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA460MT = pd.read_sql_query('SELECT * FROM "AAA460MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        SURGERY = pd.read_sql_query('SELECT * FROM "SURGERY" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        EXAM = pd.read_sql_query('SELECT * FROM "EXAM" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BOJOGI = pd.read_sql_query('SELECT * FROM "BOJOGI" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BCA201DT = pd.read_sql_query('SELECT * FROM "BCA201DT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BCA200MT = pd.read_sql_query('SELECT * FROM "BCA200MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        

    print("원천 데이터 불러오기 완료")


###############################################################################

# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}

with DAG(
    dag_id="dag_train",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    description='train model',
    #schedule="30 6 * * *",
    schedule_interval=None,
    catchup=False,
    tags=['train']
) as dag:
    
    t1 = PythonOperator(
        task_id="start_job",
        python_callable=print_text,
        op_args=["start train model"]
    )

    t2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
        op_args=[db_connect]
    )

    t3 = PythonOperator(
        task_id="end_job",
        python_callable=print_text,
        op_args=["end train model"]
    )

    t1 >> t2 >> t3


















