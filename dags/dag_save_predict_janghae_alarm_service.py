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
from datetime import datetime as dt

# t1/print_text
def print_text(text):
    print(text)

# db 연결함수
def get_db_connection():
    return psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432")

# t2/make_predict_data
def make_predict_data():
    db_connect = None
    try:
        db_connect = get_db_connection()
        # 0. 테이블 생성 쿼리
        query_create_predict_table = """
        CREATE TABLE IF NOT EXISTS "JANGHAE_ALARM_SERVICE_PREDICT_DATA" (
            "WONBU_NO" VARCHAR,
            "AGE" INT,
            "SEX" VARCHAR,
            "JAEHAEBALSAENG_HYEONGTAE_FG_CD" VARCHAR,
            "CODE_NM" VARCHAR,
            "GEUNROJA_FG" VARCHAR,
            "JONGSAJA_JIWI_CD" VARCHAR,
            "GY_HYEONGTAE_CD" VARCHAR,
            "JIKJONG_CD" VARCHAR,
            "YOYANG_ILSU" INT,
            "IPWON_BIYUL" FLOAT,
            "SANGHAE_BUWI_CD" VARCHAR,
            "SANGBYEONG_NUNIQUE" INT,
            "SANGBYEONG_CD" VARCHAR,
            "SANGSE_SANGBYEONG_NM" VARCHAR,
            "SANGBYEONG_CD_MAJOR" VARCHAR,
            "SANGBYEONG_CD_MIDDLE" VARCHAR,
            "SANGBYEONG_CD_SMALL" VARCHAR,
            "MAIN_SANGHAE_BUWI_CD" VARCHAR,
            "MAIN_SANGBYEONG_CD" VARCHAR,
            "MAIN_SANGSE_SANGBYEONG_NM" VARCHAR,
            "MAIN_SANGBYEONG_CD_MAJOR" VARCHAR,
            "MAIN_SANGBYEONG_CD_MIDDLE" VARCHAR,
            "MAIN_SANGBYEONG_CD_SMALL" VARCHAR,
            "JAEHAE_WONIN" VARCHAR,
            "GYOTONGSAGO_YN" VARCHAR,
            "SUGA_CD_COUNT" FLOAT,
            "SUGA_CD" VARCHAR,
            "EXAM_CD_COUNT" FLOAT,
            "EXAM_CD" VARCHAR,
            "BOJOGI_CD" VARCHAR,
            "FINAL_JANGHAE_GRADE" VARCHAR,
            "BUWI_8" VARCHAR,
            "BUWI_9" VARCHAR,
            "BUWI_10" VARCHAR,
            "FIRST_INPUT_ILSI" DATE,
            "LAST_CHANGE_ILSI" DATE
        );"""
