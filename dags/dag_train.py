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
        # 1-2. BCA200MT(최종장해)에서 학습용데이터에 새로 추가할 원부 추출
        # 1-3. 원천 데이터 테이블에서 새로 추가할 원부 정보만 추출
        AAA260MT = pd.read_sql_query('SELECT * FROM "AAA260MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA010MT = pd.read_sql_query('SELECT * FROM "AAA010MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA050DT = pd.read_sql_query('SELECT * FROM "AAA050DT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA230MT = pd.read_sql_query('SELECT * FROM "AAA230MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA460MT = pd.read_sql_query('SELECT * FROM "AAA460MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        SURGERY = pd.read_sql_query('SELECT * FROM "SURGERY" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        EXAM = pd.read_sql_query('SELECT * FROM "EXAM" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BOJOGI = pd.read_sql_query('SELECT * FROM "BOJOGI" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        AAA200MT = pd.read_sql_query('SELECT * FROM "AAA200MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BCA201DT = pd.read_sql_query('SELECT * FROM "BCA201DT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        BCA200MT = pd.read_sql_query('SELECT * FROM "BCA200MT" WHERE "LAST_CHANGE_ILSU"=\'2025-03-25\'', db_connect)
        # 1-4. 전처리 시 필요한 JANGHAE_GRADE_LIST 불러오기
        JANGHAE_GRADE_LIST = pd.read_sql_query('SELECT * FROM "JANGHAE_GRADE_LIST"', db_connect)
        janghae_grade_list = JANGHAE_GRADE_LIST["JANHGAE_GRADE"].to_list()

    # 2. 테이블별 전처리
    # AAA260MT 전처리
    AAA260MT = AAA260MT[(AAA260MT["AGE"] > 0)&(AAA260MT["AGE"]<100)&(AAA260MT["AGE"].notnull())] # 연령대가 1 이상, 100 미만이며 null 아닌 경우만 사용
    # AAA010MT 전처리
    AAA010MT = AAA010MT[(AAA010MT["YOYANG_JUNG_SAMANG"]!="Y")&(AAA010MT["YOYANG_ILSU"]>0)&(AAA010MT["YOYANG_ILSU"].notnull())] # 요양중 사망 제외 & 요양일수가 0보다 큰 경우만 사용 & 요양일수가 NULL이 아닌 경우만 사용
    AAA010MT = AAA010MT.drop(columns=["YOYANG_JUNG_SAMANG"]) # "YOYANG_JUNG_SAMANG" 컬럼 삭제
    AAA010MT.loc[AAA010MT["IPWON_BIYUL"] >1, "IPWON_BIYUL"] = 1 # 입원비율이 1보다 큰 경우 1로 대체
    # AAA050DT 전처리
    AAA050DT = AAA050DT[AAA050DT["SEUNGIN_FG"] == '3'] # 승인구분이 3인 데이터만 남기기
    AAA050DT.loc[AAA050DT["BOJEONG_SANGBYEONG_CD"].notnull(), "SANGBYEONG_CD"] = AAA050DT["BOJEONG_SANGBYEONG_CD"] # 보정상병코드가 null이 아닌 경우 상병코드 컬럼 null에 상관없이 상병코드 값 보정상병코드값으로 대체
    AAA050DT.loc[AAA050DT["BOJEONG_SANGBYEONG_CD"].notnull(), "SANGSE_SANGBYEONG_NM"] = AAA050DT["BOJEONG_SANGSE_SANGBYEONG_NM"] # 보정상병코드가 null이 아닌 경우 상세상병명 컬럼 null에 상관없이 상세상병명 값 보정상세상병명값으로 대체
    AAA050DT = AAA050DT[AAA050DT["SANGBYEONG_CD"].notnull()] # 대체 후 상병코드가 Null인 경우 제외
    AAA050DT["SANGBYEONG_CD_MAJOR"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: x[0]) # 상병코드(대)분류 파생변수 생성
    AAA050DT["SANGBYEONG_CD_MIDDLE"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: x[0:2]) # 상병코드(중)분류 파생변수 생성
    AAA050DT["SANGBYEONG_CD_SMALL"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: x[0:3]) # 상병코드(소)분류 파생변수 생성
    # AAA200MT_주치의소견 전처리
    AAA200MT["JANGHAE_GRADE_old"] = AAA200MT["JANGHAE_GRADE"] # 장해등급호 변수 복사
    AAA200MT["JANGHAE_GRADE"] = AAA200MT["JANGHAE_GRADE"].apply(lambda x: x[:2] if not pd.isna(x) else x) # 앞의 두자리만 추출한 장해등급 변수 생성
    # SURGERY_수술이력
    SURGERY = SURGERY[SURGERY["SUGA_CD"].notnull()] # 수술코드(SUGA_CD)가 NULL이 아닌 경우만 사용
    # EXAM_주요검사정보
    EXAM = EXAM[(EXAM["SUGA_CD"].notnull())&(EXAM["SEUNGIN_FG"]=='3')] # 검사코드가 notnull이면서, 승인구분이 3인 경우만 사용
    EXAM["JINRYO_FROM_DT_len"] = EXAM["JINRYO_FROM_DT"].fillna('0').apply(lambda x: len(x)) # 진료구분일자 결측치 0으로 처리 후 문자열 길이 변수 생성
    EXAM = EXAM[EXAM["JINRYO_FROM_DT_len"]==8].drop(columns=["JINRYO_FROM_DT_len"]) # 문자열 길이가 8인 데이터만 사용, 문자열길이 파생변수 삭제
    # BOJOGI_보조기
    BOJOGI = BOJOGI[BOJOGI["SUGA_CD"].notnull()] # 보조기코드(SUGA_CD)가 NULL이 아닌 경우만 사용
    BOJOGI["JY_DT_len"] = BOJOGI["JY_DT"].fillna('0').apply(lambda x: len(x)) # 지급일자 결측치 0으로 처리 후 문자열 길이 변수 생성
    BOJOGI = BOJOGI[BOJOGI["JY_DT_len"]==8].drop(columns=["JY_DT_len"]) # 문자열 길이가 8인 데이터만 사용, 문자열길이 파생변수 삭제
    # BCA200MT_최종장해
    BCA200MT = BCA200MT[(BCA200MT["GRADE_SANJENG_FG"]!='3')] # 가중인 경우 삭제
    BCA200MT["CHIYU_DT"] = BCA200MT["CHIYU_DT"].apply(lambda x: int(x) if not pd.isna(x) else x) # 치유일(CHIYU_DT) 컬럼 타입 INT로 변경
    BCA200MT = pd.concat([BCA200MT[(BCA200MT["CHIYU_DT"].notnull())&(BCA200MT["CHIYU_DT"]>=20080701)], BCA200MT[BCA200MT["CHIYU_DT"].isnull()]]) #(실행여부검토)치유일이 20080701 이전인 데이터 제외
    BCA200MT["JANGHAE_GRADE"] = BCA200MT["JANGHAE_GRADE"].fillna('0000') # 무장해자 "0000"으로 결측처리
    BCA200MT = BCA200MT[(BCA200MT["JANGHAE_GRADE"].isin(janghae_grade_list))] # 사용되는 장해등급호만 사용
    BCA200MT["JANGHAE_GRADE"] = BCA200MT["JANGHAE_GRADE"].apply(lambda x: x[:2] if not pd.isna(x) else x) # 장해등급 2자리로 변경
    BCA200MT["FINAL_JANGHAE_GRADE_YN"] = BCA200MT["JANGHAE_GRADE"].apply(lambda x: "Y" if x!="00" else "N")# 최종장해여부 파생변수 생성
    # BCA201DT_기초장해
    BCA201DT = BCA201DT[~(BCA201DT["GRADE_SANJENG_FG"] =='4')] # 기초장해 테이블의 등급산정구분값이 1(알반) 또는 2(준용)을 가져야 하는데 4(조정)을 갖는 경우 제외
    BCA201DT["JANGHAE_GRADE"] = BCA201DT["JANGHAE_GRADE"].fillna('0000') # 장해등급호 결측치 처리
    BCA201DT = BCA201DT[(BCA201DT["JANGHAE_GRADE"].isin(janghae_grade_list))] # 사용되는 장해등급호만 사용
    BCA201DT["JANGHAE_GRADE"] = BCA201DT["JANGHAE_GRADE"].apply(lambda x: x[:2] if not pd.isna(x) else x) # 장해등급 변수 생성
    BCA201DT = BCA201DT[~((BCA201DT["JANGHAE_GRADE"]!="00")&(BCA201DT["BUWI_FG"].isnull()))] # 기초장해등급을 보유하나 부위구분값이 없는 경우 삭제
    BCA201DT = BCA201DT[~(BCA201DT["BUWI_FG"].isin(['3','4','5','6','7','8'])&(BCA201DT["JWAU_FG"].notnull()))] # 장해부위를 3,4,5,6,7,8을 가지나 좌우구분값이 null이 아닌 경우 제외
    grade_final = list(BCA200MT.loc[BCA200MT["JANGHAE_GRADE"] != '00', "WONBU_NO"].unique()) # 앞서 전처리한 BCA200MT기준 최종장해등급 보유자 원부 리스트 생성
    BCA201DT = BCA201DT.loc[~((BCA201DT["WONBU_NO"].isin(grade_final))&(BCA201DT["BUWI_FG"].isnull())&(BCA201DT["JANGHAE_GRADE"]=="00"))] # 최종장해등급 보유자이나, 부위구분이 null이고, 기초장해등급이 "00" 인 경우 제외(최종장해등급 보유자는 부위구분에 따른 기초장해등급 반드시 존재한다고 가정)
    not_grade_final = list(BCA200MT.loc[BCA200MT["JANGHAE_GRADE"] == '00', "WONBU_NO"].unique()) # 앞서 전처리한 BCA200MT기준 무장해자 원부 리스트 생성
    BCA201DT = BCA201DT[~((BCA201DT["WONBU_NO"].isin(not_grade_final))&(BCA201DT["JANGHAE_GRADE"]!="00"))] # 최종 무장해자이나, 복수 또는 단일의 기초장해등급을 갖는 경우 제외(최종 무장해자는 기초장해등급을 가질 수 없다고 가정)
    

    print("원천 데이터 불러오기 및 전처리 완료")


###############################################################################################################################

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


















