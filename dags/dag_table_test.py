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

    # 1. 학습용데이터에 추가할 데이터 불러오기(필터링 > 원부로 불러오는 버전으로 수정)
    with db_connect.cursor() as cur:
        # 1-1. 기존 학습용데이터 테이블 4종
        TRAIN_JANGHAE_FINAL = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_FINAL"', db_connect)
        TRAIN_JANGHAE_BUWI8 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI8"', db_connect)
        TRAIN_JANGHAE_BUWI9 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI9"', db_connect)
        TRAIN_JANGHAE_BUWI10 = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_BUWI10"', db_connect)
        # 1-2. BCA200MT(최종장해)를 기준으로 학습용데이터에 추가할 원부 추출
        BCA200MT = pd.read_sql_query('SELECT "WONBU_NO" FROM "BCA200MT"', db_connect)
        insert_wonbu = ', '.join(f"'{w}'" for w in set(BCA200MT["WONBU_NO"].unique()) - set(TRAIN_JANGHAE_FINAL["WONBU_NO"].unique()))
        print(len(insert_wonbu))
        # 1-3. 원천 데이터 테이블에서 새로 추가할 원부 정보만 추출
        AAA260MT = pd.read_sql_query('SELECT * FROM "AAA260MT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(AAA260MT["WONBU_NO"].nunique())
        AAA010MT = pd.read_sql_query('SELECT * FROM "AAA010MT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(AAA010MT["WONBU_NO"].nunique())
        AAA050DT = pd.read_sql_query('SELECT * FROM "AAA050DT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(AAA050DT["WONBU_NO"].nunique())
        AAA460MT = pd.read_sql_query('SELECT * FROM "AAA460MT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(AAA460MT["WONBU_NO"].nunique())
        SURGERY = pd.read_sql_query('SELECT * FROM "SURGERY" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(SURGERY["WONBU_NO"].nunique())
        EXAM = pd.read_sql_query('SELECT * FROM "EXAM" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(EXAM["WONBU_NO"].nunique())
        BOJOGI = pd.read_sql_query('SELECT * FROM "BOJOGI" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(BOJOGI["WONBU_NO"].nunique())
        AAA200MT = pd.read_sql_query('SELECT * FROM "AAA200MT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(AAA200MT["WONBU_NO"].nunique())
        BCA201DT = pd.read_sql_query('SELECT * FROM "BCA201DT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(BCA201DT["WONBU_NO"].nunique())
        BCA200MT = pd.read_sql_query('SELECT * FROM "BCA200MT" WHERE "WONBU_NO" IN ({wonbu_list_str})', db_connect)
        print(BCA200MT["WONBU_NO"].nunique())


# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}
# DAG 정의
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