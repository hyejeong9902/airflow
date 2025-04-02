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

# SQL QUERY(전처리된 예측용 데이터셋 및 결과 테이블 저장 / 시각화 결과 표출용)
# query_create_predict_table = """
#     CREATE TABLE IF NOT EXISTS JANGHAE_JAMUN_SKIP_PREDICT_DATA (
#         WONBU_NO VARCHAR(10),
#         AGE INT,
#         SEX VERCHAR,
#         JAEHAEBALSAENG_HYEONGTAE_FG_CD VARCHAR,
#         CODE_NM VARCHAR,
#         GEUNROJA_FG VARCHAR,
#         JONGSAJA_JIWI_CD VARCHAR,
#         GY_HYEONGTAE_CD VARCHAR
#         JONGSAJA_JIWI_CD VARCHAR,
#         GY_HYEONGTAE_CD VARCHAR,
#         JIKJONG_CD VARCHAR,
#         YOYANG_ILSU INT,
#         IPWON_BIYUL FLOAT,
#         SANGHAE_BUWI_CD VARCHAR,
#         SANGBYEONG_NUNIQUE VARCHAR,
#         SANGBYEONG_CD VARCHAR,
#         SANGSE_SANGBYEONG_NM VARCHAR,
#         SANGBYEONG_CD_MAJOR VARCHAR,
#         SANGBYEONG_CD_MIDDLE VARCHAR,
#         SANGBYEONG_CD_SMALL VARCHAR,
#         MAIN_SANGHAE_BUWI_CD VARCHAR,
#         MAIN_SANGBYEONG_CD VARCHAR,
#         MAIN_SANGSE_SANGBYEONG_NM VARCHAR,
#         MAIN_SANGBYEONG_CD_MAJOR VARCHAR,
#         MAIN_SANGBYEONG_CD_MIDDLE VARCHAR,
#         MAIN_SANGBYEONG_CD_SMALL VARCHAR,
#         JAEHAE_WONIN VARCHAR,
#         GYOTONGSAGO_YN VARCHAR,
#         SUGA_CD_COUNT FLOAT,
#         SUGA_CD VARCHAR,
#         EXAM_CD_COUNT FLOAT,
#         EXAM_CD VARCHAR,
#         BOJOGI_CD VARCHAR,
#         JUCHIUI_SOGYEON VARCHAR,
#         JANGHAE_GRADE VARCHAR,
#         JANGHAE_GRADE_old VARCHAR,
#         BUWI_8 VARCHAR,
#         BUWI_9 VARCHAR,
#         BUWI_10 VARCHAR,
#         FINAL_JANGHAE_GRADE VARCHAR,
#         FIRST_INPUT_ILSI DATE,
#         LAST_CHANGE_ILSI DATE
# );"""

# t1/print_text
def print_text(text):
    print(text)

# t2/make_predict_data: 예측용 데이터셋 생성
db_connect = psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432"
    )

def make_predict_data(db_connect):
    # 1. 원천데이터에서 예측 대상자 불러오기(필터링 조건 수정 필요)
    # (as-is) 주치의소견 테이블에서 LAST_CHANGE_ILSI가 하루 전(2025-01-01)인 경우(주치의 소견에 장해등급 정보가 있으면 신청을 한 사람으로 가정)
    with db_connect.cursor() as cur:
        AAA200MT = pd.read_sql_query('SELECT * FROM "AAA200MT" WHERE "LAST_CHANGE_ILSI"=\'2025-01-01\'', db_connect)
        predict_wonbu = ', '.join(f"'{w}'" for w in set(AAA200MT["WONBU_NO"].unique()))
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
    AAA050DT = AAA050DT.sort_values(by=["WONBU_NO","SANGHAE_BUWI_CD","SANGBYEONG_FG","SANGBYEONG_CD"]).reset_index(drop=True)
    # AAA200MT_주치의소견 전처리
    AAA200MT["JANGHAE_GRADE_old"] = AAA200MT["JANGHAE_GRADE"] # 장해등급호 변수 복사
    AAA200MT["JANGHAE_GRADE"] = AAA200MT["JANGHAE_GRADE"].apply(lambda x: x[:2] if not pd.isna(x) else x) # 장해등급호 데이터에서 앞 두자리 추출한 변수 생성
    # SURGERY 전처리
    SURGERY = SURGERY[SURGERY["SUGA_CD"].notnull()].reset_index(drop=True) # 수술코드 notnull인 경우만 사용
    SURGERY = SURGERY.sort_values(by=["WONBU_NO","SUGA_CD"]) # 정렬 후 merge 필요
    # EXAM 전처리
    EXAM = EXAM[(EXAM["SUGA_CD"].notnull())&(EXAM["SEUNGIN_FG"]=='3')] # 검사코드가 notnull이면서, 승인구분이 3인 경우만 사용(승인구분 코드값 '3'인지 확인 필요)
    EXAM = EXAM.sort_values(by=["WONBU_NO","SUGA_CD"]) # 정렬 후 merge 필요
    # BOJOGI 전처리
    BOJOGI = BOJOGI[BOJOGI["SUGA_CD"].notnull()] # SUGA_CD notnull인 경우만 사용
    BOJOGI = BOJOGI.sort_values(by=["WONBU_NO","SUGA_CD"]) # 정렬 후 merge 필요

    # 3. DF 통합데이터셋 생성(필요한 데이터만 연계된다고 봐야하는 건지?)
    DF = AAA260MT.merge(AAA010MT, on="WONBU_NO", how="inner")
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGHAE_BUWI_CD.unique().apply(lambda x: ", ".join(map(str, filter(pd.notna, x)))).rename('SANGHAE_BUWI_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF["SANGHAE_BUWI_CD"] = DF["SANGHAE_BUWI_CD"].apply(lambda x: np.nan if x=='' else x) 
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGBYEONG_CD.nunique().rename('SANGBYEONG_NUNIQUE'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF["SANGBYEONG_NUNIQUE"] = DF["SANGBYEONG_NUNIQUE"].apply(lambda x: 0 if pd.isna(x) else x).astype('float') 
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGBYEONG_CD.unique().apply(lambda x: ", ".join(x)).rename('SANGBYEONG_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGSE_SANGBYEONG_NM.unique().apply(lambda x: ", ".join(map(str, x))).rename('SANGSE_SANGBYEONG_NM'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGBYEONG_CD_MAJOR.unique().apply(lambda x: ", ".join(map(str, x))).rename('SANGBYEONG_CD_MAJOR'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGBYEONG_CD_MIDDLE.unique().apply(lambda x: ", ".join(map(str, x))).rename('SANGBYEONG_CD_MIDDLE'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT.groupby('WONBU_NO').SANGBYEONG_CD_SMALL.unique().apply(lambda x: ", ".join(map(str, x))).rename('SANGBYEONG_CD_SMALL'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGHAE_BUWI_CD.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGHAE_BUWI_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGBYEONG_CD.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGBYEONG_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGSE_SANGBYEONG_NM.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGSE_SANGBYEONG_NM'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGBYEONG_CD_MAJOR.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGBYEONG_CD_MAJOR'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGBYEONG_CD_MIDDLE.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGBYEONG_CD_MIDDLE'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, AAA050DT[AAA050DT["SANGBYEONG_FG"]=='1'].groupby('WONBU_NO').SANGBYEONG_CD_SMALL.unique().apply(lambda x: ", ".join(map(str, x))).rename('MAIN_SANGBYEONG_CD_SMALL'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = DF.merge(AAA230MT, on="WONBU_NO", how="inner")
    DF = DF.merge(AAA460MT, on="WONBU_NO", how="inner")
    DF = pd.merge(DF, SURGERY.groupby('WONBU_NO').SUGA_CD.count().rename('SUGA_CD_COUNT'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, SURGERY.groupby('WONBU_NO').SUGA_CD.unique().apply(lambda x: ", ".join(x)).rename('SUGA_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, EXAM.groupby('WONBU_NO').SUGA_CD.count().rename('EXAM_CD_COUNT'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, EXAM.groupby('WONBU_NO').SUGA_CD.unique().apply(lambda x: ", ".join(x)).rename('EXAM_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = pd.merge(DF, BOJOGI.groupby('WONBU_NO').SUGA_CD.unique().apply(lambda x: ", ".join(x)).rename('BOJOGI_CD'),how='left',left_on='WONBU_NO',right_on='WONBU_NO')
    DF = DF.merge(AAA200MT, on="WONBU_NO", how="inner")
    DF = DF.reset_index(drop=True)

    # 데이터타입 확인
    int_col = ['AGE', 'YOYANG_ILSU', 'SANGBYEONG_NUNIQUE']
    float_col = ['IPWON_BIYUL', 'SUGA_CD_COUNT', 'EXAM_CD_COUNT']
    var_col = ['SEX', 'JAEHAEBALSAENG_HYEONGTAE_FG_CD', 'CODE_NM','GEUNROJA_FG', 'JONGSAJA_JIWI_CD', 'GY_HYEONGTAE_CD', 'JIKJONG_CD',
               'SANGHAE_BUWI_CD', 'SANGBYEONG_CD', 'SANGSE_SANGBYEONG_NM','SANGBYEONG_CD_MAJOR', 'SANGBYEONG_CD_MIDDLE', 'SANGBYEONG_CD_SMALL',
               'MAIN_SANGHAE_BUWI_CD', 'MAIN_SANGBYEONG_CD','MAIN_SANGSE_SANGBYEONG_NM', 'MAIN_SANGBYEONG_CD_MAJOR','MAIN_SANGBYEONG_CD_MIDDLE', 'MAIN_SANGBYEONG_CD_SMALL',
               'JAEHAE_WONIN','GYOTONGSAGO_YN', 'SUGA_CD', 'EXAM_CD', 'BOJOGI_CD', 'JUCHIUI_SOGYEON',
               'JANGHAE_GRADE', 'JANGHAE_GRADE_old']
    
    for i in DF.columns:
        if i in int_col: # int에 해당하는 컬럼들은 위 단계에서 결측인경우 제외 또는 0으로 처리해서 결측치없음
            DF[i] = DF[i].astype('int')
        elif i in float_col:
            DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('float')
        elif i in var_col:
            DF.loc[DF[i].notna(),i] = DF.loc[DF[i].notna(),i].astype('str')
    DF = DF[["WONBU_NO"]+int_col+float_col+var_col]

    DF["FIRST_INPUT_ILSI"] = dt.today()
    DF["LAST_CHANGE_ILSI"] = dt.today()

    return DF

#   t3/ : 모델 로드 및 예측
def load_moedl(db_connect):
    df = make_predict_data(db_connect)
    print(df.shape)


 ########################################################################################################

# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}
# DAG 정의
with DAG(
    dag_id="dag_save_predict_janghae_jamun_skip",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    description='predict model',
    #schedule="30 6 * * *",
    schedule_interval=None,
    catchup=False,
    tags=['predict']
) as dag:
    
    t1 = PythonOperator(
        task_id="start_job",
        python_callable=print_text,
        op_args=["start train model"]
    )

    t2 = PythonOperator(
        task_id="make_predict_data",
        python_callable=make_predict_data,
        op_args=[db_connect]
    )

    t3 = PythonOperator(
        task_id="load_moedl",
        python_callable=load_moedl,
        op_args=[db_connect]
    )

    t4 = PythonOperator(
        task_id="end_job",
        python_callable=print_text,
        op_args=["end train model"]
    )

    t1 >> t2 >> t3 >> t4