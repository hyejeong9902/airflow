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

        # 1. 원천데이터에서 예측 대상자 불러오기(필터링 조건 수정 필요!!!!!)
        # (as-is) AAA240MT(정렬요양기간)의 
        with db_connect.cursor() as cur:
            AAA260MT = pd.read_sql_query('SELECT * FROM "AAA260MT" WHERE "LAST_CHANGE_ILSI"=\'2025-01-01\'', db_connect) # "LAST_CHANGE_ILSI"= CURRENT_DATE - 1'
            predict_wonbu = ', '.join(f"'{w}'" for w in set(AAA260MT["WONBU_NO"].unique()))

            # 빈 데이터셋 체크
            if not predict_wonbu:
                print("예측 대상자가 없습니다.")
                db_connect.close()
                return

            # 원천데이터 테이블에서 새로 추가할 원부 정보만 추출(나머지 테이블에서 받아오기)
            AAA260MT = pd.read_sql_query(f'SELECT * FROM "AAA260MT" WHERE "WONBU_NO" IN ({predict_wonbu})', db_connect)
            AAA010MT = pd.read_sql_query(f'SELECT * FROM "AAA010MT" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            AAA050DT = pd.read_sql_query(f'SELECT * FROM "AAA050DT" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            AAA230MT = pd.read_sql_query(f'SELECT * FROM "AAA230MT" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            AAA460MT = pd.read_sql_query(f'SELECT * FROM "AAA460MT" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            SURGERY = pd.read_sql_query(f'SELECT * FROM "SURGERY" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            EXAM = pd.read_sql_query(f'SELECT * FROM "EXAM" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect)
            BOJOGI = pd.read_sql_query(f'SELECT * FROM "BOJOGI" WHERE "WONBU_NO" in ({predict_wonbu})', db_connect) 


            # 2. 테이블별 전처리
            # AAA260MT_급여원부정보 전처리
            AAA260MT["AGE"] = AAA260MT["AGE"].fillna(1) # 연령 변수 결측처리
            # AAA010MT_최초요양급여신청서 전처리
            AAA010MT["YOYANG_ILSU"] = AAA010MT["YOYANG_ILSU"].fillna(1) # 요양일수 변수 결측처리
            AAA010MT.loc[AAA010MT["IPWON_BIYUL"] >1, "IPWON_BIYUL"] = 1 # 입원비율이 1보다 큰 경우 1로 대체
            # AAA050DT_급야원부상병검색 전처리
            AAA050DT = AAA050DT[AAA050DT["SEUNGIN_FG"].astype(str) == '3'] # 승인구분이 '3'인 데이터만 남기기
            AAA050DT.loc[AAA050DT["BOJEONG_SANGBYEONG_CD"].notnull(), "SANGBYEONG_CD"] = AAA050DT["BOJEONG_SANGBYEONG_CD"] # 보정상병코드가 null이 아닌 경우 상병코드 컬럼 null에 상관없이 모두 보정상병값으로 대체(상병코드)
            AAA050DT.loc[AAA050DT["BOJEONG_SANGBYEONG_CD"].notnull(), "SANGSE_SANGBYEONG_NM"] = AAA050DT["BOJEONG_SANGSE_SANGBYEONG_NM"] # 보정상병코드가 null이 아닌 경우 상병코드 컬럼 null에 상관없이 모두 보정상병값으로 대체(상세상병명)
            AAA050DT = AAA050DT[AAA050DT["SANGBYEONG_CD"].notnull()].reset_index(drop=True) # 대체 후 상병코드가 Null인 경우 제외
            AAA050DT["SANGBYEONG_CD_MAJOR"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0]) # 상병코드(대) 파생변수 생성
            AAA050DT["SANGBYEONG_CD_MIDDLE"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0:2]) # 상병코드(중) 파생변수 생성
            AAA050DT["SANGBYEONG_CD_SMALL"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0:3]) # 상병코드(소) 파생변수 생성
            AAA050DT["SANGHAE_BUWI_CD"] = AAA050DT["SANGHAE_BUWI_CD"].apply(lambda x: x.zfill(2) if not pd.isna(x) else x) # (실행여부검토필요)공단코드와 동일하게 맞추기
            # SURGERY 전처리
            SURGERY = SURGERY[SURGERY["SUGA_CD"].notnull()].reset_index(drop=True) # 수술코드 notnull인 경우만 사용
            # EXAM 전처리
            EXAM = EXAM[(EXAM["SUGA_CD"].notnull())&(EXAM["SEUNGIN_FG"]=='3')] # 검사코드가 notnull이면서, 승인구분이 3인 경우만 사용(승인구분 코드값 '3'인지 확인 필요)
            EXAM["JINRYO_FROM_DT_len"] = EXAM["JINRYO_FROM_DT"].fillna('0').apply(lambda x: len(x)) # 진료구분일자가 8글자로 제대로 표기되어 있는 경우만 사용
            EXAM = EXAM[EXAM["JINRYO_FROM_DT_len"]==8].drop(columns=["JINRYO_FROM_DT_len"]).reset_index(drop=True)
            # BOJOGI 전처리
            BOJOGI = BOJOGI[BOJOGI["SUGA_CD"].notnull()] # SUGA_CD notnull인 경우만 사용
            BOJOGI["JY_DT_len"] = BOJOGI["JY_DT"].fillna('0').apply(lambda x: len(x)) # 보조기 지급날짜가 8글자로 표기되어 있는 경우만 사용
            BOJOGI = BOJOGI[BOJOGI["JY_DT_len"]==8].drop(columns=["JY_DT_len"]).reset_index(drop=True)