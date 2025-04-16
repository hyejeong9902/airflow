import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import os
from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
# from dotenv import load_dotenv

# # 환경 변수 load
# load_dotenv()

# # db 연결 함수
# def get_db_connection():
#     return psycopg2.connect(
#         database=os.getenv('DB_NAME'),
#         user=os.getenv('DB_USER'),
#         password=os.getenv('DB_PASSWORD'),
#         host=os.getenv('DB_HOST'),
#         port=os.getenv('DB_PORT')
#     )

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
        CREATE TABLE IF NOT EXISTS "JANGHAE_JAMUN_SKIP_PREDICT_DATA" (
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
            "JUCHIUI_SOGYEON" VARCHAR,
            "JANGHAE_GRADE" VARCHAR,
            "JANGHAE_GRADE_old" VARCHAR,
            "BUWI_8" VARCHAR,
            "BUWI_9" VARCHAR,
            "BUWI_10" VARCHAR,
            "FINAL_JANGHAE_GRADE" VARCHAR,
            "FIRST_INPUT_ILSI" DATE,
            "LAST_CHANGE_ILSI" DATE
        );"""

        # 1. 원천데이터에서 예측 대상자 불러오기(필터링 조건 수정 필요)/일 300명 / 필터링 정보 확인 필요
        # 매일 자정 수행된다고 가정했을 때(schedule_interval="@daily") 하루 전에 들어온 데이터 활용("LAST_CHANGE_ILSI"= CURRENT_DATE - 1')
        # (as-is) 주치의소견 테이블에서 LAST_CHANGE_ILSI가 하루 전(2025-01-01)인 경우(주치의 소견에 장해등급 정보가 있으면 신청을 한 사람으로 가정)
        with db_connect.cursor() as cur:
            AAA200MT = pd.read_sql_query('SELECT * FROM "AAA200MT" WHERE "LAST_CHANGE_ILSI"=\'2025-01-01\'', db_connect) # "LAST_CHANGE_ILSI"= CURRENT_DATE - 1'
            predict_wonbu = ', '.join(f"'{w}'" for w in set(AAA200MT["WONBU_NO"].unique()))

            # 빈 데이터셋 체크
            if not predict_wonbu:
                print("예측 대상자가 없습니다.")
                db_connect.close()
                return 
        
            # 원천데이터 테이블에서 새로 추가할 원부 정보만 추출(나머지 테이블에서 받아오기) / 평균 300명(일)
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
        AAA050DT = AAA050DT[AAA050DT["SANGBYEONG_CD"].notnull()] # 대체 후 상병코드가 Null인 경우 제외
        AAA050DT["SANGBYEONG_CD_MAJOR"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0]) # 상병코드(대) 파생변수 생성
        AAA050DT["SANGBYEONG_CD_MIDDLE"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0:2]) # 상병코드(중) 파생변수 생성
        AAA050DT["SANGBYEONG_CD_SMALL"] = AAA050DT["SANGBYEONG_CD"].map(lambda x: str(x)[0:3]) # 상병코드(소) 파생변수 생성
        AAA050DT = AAA050DT.sort_values(by=["WONBU_NO","SANGHAE_BUWI_CD","SANGBYEONG_FG","SANGBYEONG_CD"]).reset_index(drop=True) # 정렬 후 merge 필요
        # AAA200MT_주치의소견 전처리
        AAA200MT["JANGHAE_GRADE_old"] = AAA200MT["JANGHAE_GRADE"] # 장해등급호 변수 복사
        AAA200MT["JANGHAE_GRADE"] = AAA200MT["JANGHAE_GRADE"].apply(lambda x: x[:2] if not pd.isna(x) else x) # 장해등급호 데이터에서 앞 두자리 추출한 변수 생성
        # SURGERY 전처리
        SURGERY = SURGERY[SURGERY["SUGA_CD"].notnull()] # 수술코드 notnull인 경우만 사용
        SURGERY = SURGERY.sort_values(by=["WONBU_NO","SUGA_CD"]).reset_index(drop=True) # 정렬 후 merge 필요
        # EXAM 전처리
        EXAM = EXAM[(EXAM["SUGA_CD"].notnull())&(EXAM["SEUNGIN_FG"]=='3')] # 검사코드가 notnull이면서, 승인구분이 3인 경우만 사용(승인구분 코드값 '3'인지 확인 필요)
        EXAM = EXAM.sort_values(by=["WONBU_NO","SUGA_CD"]).reset_index(drop=True) # 정렬 후 merge 필요
        # BOJOGI 전처리
        BOJOGI = BOJOGI[BOJOGI["SUGA_CD"].notnull()] # SUGA_CD notnull인 경우만 사용
        BOJOGI = BOJOGI.sort_values(by=["WONBU_NO","SUGA_CD"]).reset_index(drop=True) # 정렬 후 merge 필요

        # 3. DF 통합데이터셋 생성
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
        DF = DF.where(pd.notnull(DF), None) # DB에 들어갈 결측치 처리

        # 데이터타입 확인(DB 저장용)
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

        # DB에 저장
        # db에 테이블 저장 안되어 있을 때 생성하는 함수
        def _exec_query(conn, query):
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        # 이전 실행으로 db에 저장된 테이블이 있다면 해당 테이블에 데이터 누적하는 함수
        def _execute_values(conn, df, table_name):
            df['FIRST_INPUT_ILSI'] = dt.today() # 예측 시 가져오는 데이터는 전처리된 오늘 날짜 기준
            df['LAST_CHANGE_ILSI'] = dt.today() # 예측 시 가져오는 데이터는 전처리된 오늘 날짜 기준

            tuples = [tuple(x) for x in df.to_numpy()] 
            cols = ', '.join([f'"{col}"' for col in df.columns])  # ','.join(list(df.columns))
            query = f'INSERT INTO "{table_name}" ({cols}) VALUES %s' # "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols) 
            with conn.cursor() as cur: 
                extras.execute_values(cur, query, tuples)
                conn.commit()
    
        with db_connect.cursor() as cur:
            # 테이블이 없으면 생성
            _exec_query(conn=db_connect, query=query_create_predict_table) # JANGHAE_JAMUN_SKIP_PREDICT_DATA 테이블 생성
            # 있다면 기존 테이블에 값 추가(if 새로운 원부가 들어온다면?)
            _execute_values(conn=db_connect, df=DF, table_name="JANGHAE_JAMUN_SKIP_PREDICT_DATA")
    
        print(f"예측 데이터 생성 완료: 총 {len(DF)}건")

    except Exception as e: # 어떤 오류인지 나오는 건지?
        print(f"예측 데이터 생성 중 오류 발생: {e}")
        raise
    
    finally:
        db_connect.close()

# BashOperator 수행을 위한 스크립트 파일 생성
scripts_dir = "/opt/airflow/scripts"
os.makedirs(scripts_dir, exist_ok=True)

# t3/predict_janhgae_grade_spine
with open(f"{scripts_dir}/predict_spine.py", "w") as f:
    f.write('''#!/usr/bin/env python
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import sys
import os
from autogluon.tabular import TabularPredictor

# 인자로 전달된 모델 경로
save_path = sys.argv[1]

# db 연결함수(환경변수 연결로 변경 필요)
def get_db_connection():
    return psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432")


# 예측값 DB UPDATE 함수
def update_prediction_results(conn, df):
    try:
        update_query = """UPDATE "JANGHAE_JAMUN_SKIP_PREDICT_DATA" SET "BUWI_8" = %s WHERE "WONBU_NO" = %s"""
        data_to_update = list(zip(df["BUWI_8"].astype(str), df["WONBU_NO"].astype(str)))
        with conn.cursor() as cur:
            extras.execute_batch(cur, update_query, data_to_update)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating BUWI_8: {e}")
        raise

# 메인 실행 함수
def predict_janhgae_grade_spine():
    db_connect = None
    try:
        db_connect = get_db_connection()
        # 데이터 불러오기
        with db_connect.cursor() as cur:
            DF = pd.read_sql_query('SELECT * FROM "JANGHAE_JAMUN_SKIP_PREDICT_DATA" WHERE "LAST_CHANGE_ILSI"= CURRENT_DATE', db_connect)
        
        # 빈 데이터셋 체크
        if len(DF) == 0:
            print("예측할 데이터가 없습니다.")
            return
        
        # 예측에서 제외할 컬럼
        del_col = ['WONBU_NO','BUWI_8', 'BUWI_9', 'BUWI_10', 'FINAL_JANGHAE_GRADE', 'FIRST_INPUT_ILSI', 'LAST_CHANGE_ILSI'
                   # 장해부위 척주 예측에 사용되지 않는 변수 
                   'GEUNROJA_FG', 'JONGSAJA_JIWI_CD', 'GY_HYEONGTAE_CD',
                   'SANGSE_SANGBYEONG_NM', 'SANGBYEONG_CD', 'MAIN_SANGSE_SANGBYEONG_NM', 'MAIN_SANGBYEONG_CD_MAJOR',
                   'GYOTONGSAGO_YN']
        df_drop = DF.drop(columns=del_col)
        
        # 데이터 타입 확인
        int_col = ['AGE', 'YOYANG_ILSU', 'SANGBYEONG_NUNIQUE']
        float_col = ['IPWON_BIYUL', 'SUGA_CD_COUNT', 'EXAM_CD_COUNT']
        category_col = ['SEX', 'JAEHAEBALSAENG_HYEONGTAE_FG_CD', 'GEUNROJA_FG', 'JONGSAJA_JIWI_CD', 'GY_HYEONGTAE_CD', 'GYOTONGSAGO_YN', 'JANGHAE_GRADE']
        object_col = ['CODE_NM','JIKJONG_CD',
                     'SANGHAE_BUWI_CD', 'SANGBYEONG_CD', 'SANGSE_SANGBYEONG_NM','SANGBYEONG_CD_MAJOR', 'SANGBYEONG_CD_MIDDLE', 'SANGBYEONG_CD_SMALL',
                     'MAIN_SANGHAE_BUWI_CD', 'MAIN_SANGBYEONG_CD','MAIN_SANGSE_SANGBYEONG_NM', 'MAIN_SANGBYEONG_CD_MAJOR','MAIN_SANGBYEONG_CD_MIDDLE', 'MAIN_SANGBYEONG_CD_SMALL',
                     'JAEHAE_WONIN','SUGA_CD', 'EXAM_CD', 'BOJOGI_CD', 'JUCHIUI_SOGYEON',
                     'JANGHAE_GRADE_old']
        
        for col in df_drop.columns:
            if col in int_col:
                df_drop[col] = df_drop[col].astype('int')
            elif col in category_col:
                df_drop[col] = df_drop[col].astype('category')
            elif col in float_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('float')
            elif col in object_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('str')
        
        # 모델 로드 및 예측
        predictor = TabularPredictor.load(path=save_path)
        pre = predictor.predict(df_drop)
        pre_proba = predictor.predict_proba(df_drop)
        
        # 임계값 설정 - 클래스 간 확률차이 계산
        positive_class = "14"
        negative_class = "00"
        pre_proba["diff"] = pre_proba[positive_class] - pre_proba[negative_class]
        pre_series = pre.copy()
        mask = (pre_series == positive_class) & (pre_proba["diff"] < 0.5) # threshold=0.5
        pre_series[mask] = negative_class
        
        # 예측 결과 데이터프레임 생성
        result_df = pd.DataFrame({f"BUWI_8": pre_series})
        
        # 원본 WONBU_NO와 결합
        update_df = pd.DataFrame({
            "WONBU_NO": DF["WONBU_NO"],
            "BUWI_8": result_df["BUWI_8"]
        })
        
        # 테이블에 예측값 업데이트
        update_prediction_results(db_connect, update_df)
        
        print("장해부위 척주 예측 완료")
        
    except Exception as e:
        print(f"장해부위 척주 예측 중 오류 발생: {e}")
        
    finally:
        if db_connect:
            db_connect.close()

# 스크립트 실행
if __name__ == "__main__":
    predict_janhgae_grade_spine() # taskid
''')

# t4/predict_janhgae_grade_arms
with open(f"{scripts_dir}/predict_arms.py", "w") as f:
    f.write('''#!/usr/bin/env python
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import sys
import os
from autogluon.tabular import TabularPredictor

# 인자로 전달된 모델 경로
save_path = sys.argv[1] 

# db 연결함수(환경변수 연결로 변경 필요)
def get_db_connection():
    return psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432")

# 예측값 DB UPDATE 함수
def update_prediction_results(conn, df):
    try:
        update_query = """UPDATE "JANGHAE_JAMUN_SKIP_PREDICT_DATA" SET "BUWI_9" = %s WHERE "WONBU_NO" = %s"""
        data_to_update = list(zip(df["BUWI_9"].astype(str), df["WONBU_NO"].astype(str)))
        with conn.cursor() as cur:
            extras.execute_batch(cur, update_query, data_to_update)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating BUWI_9: {e}")
        raise

# 메인 실행 함수
def predict_janhgae_grade_arms():
    db_connect = None
    try:
        db_connect = get_db_connection()
        # 데이터 불러오기
        with db_connect.cursor() as cur:
            DF = pd.read_sql_query('SELECT * FROM "JANGHAE_JAMUN_SKIP_PREDICT_DATA" WHERE "LAST_CHANGE_ILSI" = CURRENT_DATE', db_connect)
        
        # 빈 데이터셋 체크
        if len(DF) == 0:
            print("예측할 데이터가 없습니다.")
            return
        
        # 예측에서 제외할 컬럼
        del_col = ['WONBU_NO', 'BUWI_8', 'BUWI_9', 'BUWI_10', 'FINAL_JANGHAE_GRADE', 'FIRST_INPUT_ILSI', 'LAST_CHANGE_ILSI',
                   # 장해부위 팔 예측에 사용되지 않는 변수
                   'CODE_NM','JONGSAJA_JIWI_CD']
        df_drop = DF.drop(columns=del_col)
        
        # 데이터 타입 확인
        int_col = ['AGE', 'YOYANG_ILSU', 'SANGBYEONG_NUNIQUE']
        float_col = ['IPWON_BIYUL', 'SUGA_CD_COUNT', 'EXAM_CD_COUNT']
        category_col = ['SEX', 'JAEHAEBALSAENG_HYEONGTAE_FG_CD', 'GEUNROJA_FG', 'JONGSAJA_JIWI_CD', 'GY_HYEONGTAE_CD', 'GYOTONGSAGO_YN', 'JANGHAE_GRADE']
        object_col = ['CODE_NM','JIKJONG_CD',
                     'SANGHAE_BUWI_CD', 'SANGBYEONG_CD', 'SANGSE_SANGBYEONG_NM','SANGBYEONG_CD_MAJOR', 'SANGBYEONG_CD_MIDDLE', 'SANGBYEONG_CD_SMALL',
                     'MAIN_SANGHAE_BUWI_CD', 'MAIN_SANGBYEONG_CD','MAIN_SANGSE_SANGBYEONG_NM', 'MAIN_SANGBYEONG_CD_MAJOR','MAIN_SANGBYEONG_CD_MIDDLE', 'MAIN_SANGBYEONG_CD_SMALL',
                     'JAEHAE_WONIN','SUGA_CD', 'EXAM_CD', 'BOJOGI_CD', 'JUCHIUI_SOGYEON',
                     'JANGHAE_GRADE_old']
        
        for col in df_drop.columns:
            if col in int_col:
                df_drop[col] = df_drop[col].astype('int')
            elif col in category_col:
                df_drop[col] = df_drop[col].astype('category')
            elif col in float_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('float')
            elif col in object_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('str')
        
        # 모델 로드 및 예측
        predictor = TabularPredictor.load(path=save_path)
        pre = predictor.predict(df_drop)
        pre_proba = predictor.predict_proba(df_drop)
        
        # 임계값 설정 - 클래스 간 확률차이 계산
        positive_class = "14"
        negative_class = "00"
        pre_proba["diff"] = pre_proba[positive_class] - pre_proba[negative_class]        
        pre_series = pre.copy()
        mask = (pre_series == positive_class) & (pre_proba["diff"] < 0.5) # threshold=0.5
        pre_series[mask] = negative_class
        
        # 예측 결과 데이터프레임 생성
        result_df = pd.DataFrame({f"BUWI_9": pre_series})
        
        # 원본 WONBU_NO와 결합
        update_df = pd.DataFrame({
            "WONBU_NO": DF["WONBU_NO"],
            "BUWI_9": result_df["BUWI_9"]
        })
        
        # 테이블에 예측값 업데이트
        update_prediction_results(db_connect, update_df)
        
        print("장해부위 팔 예측 완료")
        
    except Exception as e:
        print(f"장해부위 팔 예측 중 오류 발생: {e}")
        
    finally:
        if db_connect:
            db_connect.close()

# 스크립트 실행
if __name__ == "__main__":
    predict_janhgae_grade_arms() # taskid
''')

# t5/predict_janhgae_grade_legs
with open(f"{scripts_dir}/predict_legs.py", "w") as f:
    f.write('''#!/usr/bin/env python
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import sys
import os
from autogluon.tabular import TabularPredictor

# 인자로 전달된 모델 경로
save_path = sys.argv[1]

# db 연결함수(환경변수 연결로 변경 필요)
def get_db_connection():
    return psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432")

# 예측값 DB UPDATE 함수
def update_prediction_results(conn, df):
    try:
        update_query = """UPDATE "JANGHAE_JAMUN_SKIP_PREDICT_DATA" SET "BUWI_10" = %s WHERE "WONBU_NO" = %s"""
        data_to_update = list(zip(df["BUWI_10"].astype(str), df["WONBU_NO"].astype(str)))
        with conn.cursor() as cur:
            extras.execute_batch(cur, update_query, data_to_update)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating BUWI_10: {e}")
        raise

# 메인 실행 함수
def predict_janhgae_grade_legs():
    db_connect = None
    try:
        db_connect = get_db_connection()
        # 데이터 불러오기
        with db_connect.cursor() as cur:
            DF = pd.read_sql_query('SELECT * FROM "JANGHAE_JAMUN_SKIP_PREDICT_DATA" WHERE "LAST_CHANGE_ILSI" = CURRENT_DATE', db_connect)
        
        # 빈 데이터셋 체크
        if len(DF) == 0:
            print("예측할 데이터가 없습니다.")
            return
        
        # 예측에서 제외할 컬럼
        del_col = ['WONBU_NO', 'BUWI_8', 'BUWI_9', 'BUWI_10', 'FINAL_JANGHAE_GRADE', 'FIRST_INPUT_ILSI', 'LAST_CHANGE_ILSI',
                   # 장해부위 다리 예측에 사용되지 않는 변수
                   'GEUNROJA_FG','JONGSAJA_JIWI_CD',
                   'SANGSE_SANGBYEONG_NM','MAIN_SANGBYEONG_CD_MAJOR',
                   'GYOTONGSAGO_YN','JUCHIUI_SOGYEON']
        df_drop = DF.drop(columns=del_col)
        
        # 데이터 타입 확인
        int_col = ['AGE', 'YOYANG_ILSU', 'SANGBYEONG_NUNIQUE']
        float_col = ['IPWON_BIYUL', 'SUGA_CD_COUNT', 'EXAM_CD_COUNT']
        category_col = ['SEX', 'JAEHAEBALSAENG_HYEONGTAE_FG_CD', 'GEUNROJA_FG', 'JONGSAJA_JIWI_CD', 'GY_HYEONGTAE_CD', 'GYOTONGSAGO_YN', 'JANGHAE_GRADE']
        object_col = ['CODE_NM','JIKJONG_CD',
                     'SANGHAE_BUWI_CD', 'SANGBYEONG_CD', 'SANGSE_SANGBYEONG_NM','SANGBYEONG_CD_MAJOR', 'SANGBYEONG_CD_MIDDLE', 'SANGBYEONG_CD_SMALL',
                     'MAIN_SANGHAE_BUWI_CD', 'MAIN_SANGBYEONG_CD','MAIN_SANGSE_SANGBYEONG_NM', 'MAIN_SANGBYEONG_CD_MAJOR','MAIN_SANGBYEONG_CD_MIDDLE', 'MAIN_SANGBYEONG_CD_SMALL',
                     'JAEHAE_WONIN','SUGA_CD', 'EXAM_CD', 'BOJOGI_CD', 'JUCHIUI_SOGYEON',
                     'JANGHAE_GRADE_old']
        
        for col in df_drop.columns:
            if col in int_col:
                df_drop[col] = df_drop[col].astype('int')
            elif col in category_col:
                df_drop[col] = df_drop[col].astype('category')
            elif col in float_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('float')
            elif col in object_col:
                df_drop.loc[df_drop[col].notna(),col] = df_drop.loc[df_drop[col].notna(),col].astype('str')
        
        # 모델 로드 및 예측
        predictor = TabularPredictor.load(path=save_path)
        pre = predictor.predict(df_drop)
        pre_proba = predictor.predict_proba(df_drop)
        
        # 임계값 설정 - 클래스 간 확률차이 계산
        positive_class = "14"
        negative_class = "00"
        pre_proba["diff"] = pre_proba[positive_class] - pre_proba[negative_class]
        pre_series = pre.copy()
        mask = (pre_series == positive_class) & (pre_proba["diff"] < 0.5) # threshold=0.5
        pre_series[mask] = negative_class
        
        # 예측 결과 데이터프레임 생성
        result_df = pd.DataFrame({f"BUWI_10": pre_series})
        
        # 원본 WONBU_NO와 결합
        update_df = pd.DataFrame({
            "WONBU_NO": DF["WONBU_NO"],
            "BUWI_10": result_df["BUWI_10"]
        })
        
        # 테이블에 예측값 업데이트
        update_prediction_results(db_connect, update_df)
        
        print("장해부위 다리 예측 완료")
        
    except Exception as e:
        print(f"장해부위 다리 예측 중 오류 발생: {e}")
        
    finally:
        if db_connect:
            db_connect.close()

# 스크립트 실행
if __name__ == "__main__":
    predict_janhgae_grade_legs() # taskid
''')

# 스크립트에 실행 권한 부여
os.chmod(f"{scripts_dir}/predict_spine.py", 0o755)
os.chmod(f"{scripts_dir}/predict_arms.py", 0o755)
os.chmod(f"{scripts_dir}/predict_legs.py", 0o755)


# t6/predict_final_grade : 각 부위별 예측 기초장해등급으로 룰기반 최종장해등급 도출
def predict_final_grade():
    db_connect = None
    try:
        db_connect = get_db_connection()
        with db_connect.cursor() as cur:
            DF = pd.read_sql_query('SELECT * FROM "JANGHAE_JAMUN_SKIP_PREDICT_DATA" WHERE "LAST_CHANGE_ILSI"= CURRENT_DATE', db_connect)
        # 빈 데이터셋 체크
        if len(DF) == 0:
            print("예측할 데이터가 없습니다.")
            return
        # 최종장해등급 산출(룰 기반)
        # step1. 원부별 장해등급 리스트 가져오기    
        DF['GRADE_LIST'] = DF[['BUWI_8','BUWI_9','BUWI_10']].values.tolist()
        DF['GRADE_LIST'] = DF['GRADE_LIST'].apply(lambda x: sorted(x))
        # step2-1. "00"이 2개 이상인 경우 역순 정렬 후 첫번째 자리 추출
        condition1 = (DF['GRADE_LIST'].astype('str').str.findall('00').str.len()>=2)
        DF.loc[condition1,'FINAL_JANGHAE_GRADE'] = DF.loc[condition1,'GRADE_LIST'].str[-1]
        # step2-2. "14"가 3개 이거나 또는 "14"가 2개, "00"이 1개인 경우
        condition2_1 = (DF['GRADE_LIST'].astype('str').str.findall('14').str.len()==3)
        condition2_2 = ((DF['GRADE_LIST'].astype('str').str.findall('14').str.len()==2) &
                        (DF['GRADE_LIST'].astype('str').str.findall('00').str.len()==1))
        DF.loc[condition2_1|condition2_2,'FINAL_JANGHAE_GRADE'] = '14'
        # step2-3. "14"가 2개, 나머지 1개는 "00"이 아닌 경우
        condition3 = ((DF['GRADE_LIST'].astype('str').str.findall('14').str.len()==2) & 
                      (DF['GRADE_LIST'].astype('str').str.findall('00').str.len()==0))
        DF.loc[condition3,'FINAL_JANGHAE_GRADE'] = DF.loc[condition3,'GRADE_LIST'].str[0]
        # step2-4. "00"과 "14"를 각각 1개씩 갖는 경우 정렬 후 두번째 자리 추출
        condition4 = ((DF['GRADE_LIST'].astype('str').str.findall('14').str.len()==1) & 
                      (DF['GRADE_LIST'].astype('str').str.findall('00').str.len()==1))
        DF.loc[condition4,'FINAL_JANGHAE_GRADE'] = DF.loc[condition4,'GRADE_LIST'].str[1]
        # step2-5. "00"이나 "14"가 1개 이하인 경우(기본적으로 장해등급을 2~3개 가짐)
        # 2-5-1. 5등급 이하가 2개 이상인 경우(첫번째 자리 숫자가 03이하면 "01"로 추출)
        condition5_1 = (DF['GRADE_LIST'].apply(lambda x: len([i for i in x if (i <= '05') & (i not in ['00','14'])])>=2))
        DF.loc[condition5_1,'FINAL_JANGHAE_GRADE'] = DF.loc[condition5_1,'GRADE_LIST'].apply(lambda x: '01' if [i for i in x if i not in ['00','14']][0]<='03' else str(int([i for i in x if i not in ['00','14']][0])-3).zfill(2))
        # 2-5-2. 8등급 이하가 2개 이상인 경우(첫번째 자리 숫자가 02이하면 "01"로 추출)
        condition5_2 = (DF['GRADE_LIST'].apply(lambda x: len([i for i in x if (i <= '08') & (i not in ['00','14'])])>=2))
        DF.loc[~condition5_1&condition5_2,'FINAL_JANGHAE_GRADE'] = DF.loc[~condition5_1&condition5_2,'GRADE_LIST'].apply(lambda x: '01' if [i for i in x if i not in ['00','14']][0]<='02' else str(int([i for i in x if i not in ['00','14']][0])-2).zfill(2))
        # 2-5-3. 13등급 이하가 2개 이상인 경우(첫번째 자리 숫자가 1이면 "01"로 추출)
        condition5_3 = (DF['GRADE_LIST'].apply(lambda x: len([i for i in x if (i <= '13') & (i not in ['00','14'])])>=2))
        DF.loc[~condition5_1&~condition5_2&condition5_3,'FINAL_JANGHAE_GRADE'] = DF.loc[~condition5_1&~condition5_2&condition5_3,'GRADE_LIST'].apply(lambda x: '01' if [i for i in x if i not in ['00','14']][0]<='01' else str(int([i for i in x if i not in ['00','14']][0])-1).zfill(2))

        DF = DF.drop(['GRADE_LIST'],axis=1)

        # 업뎃 시 필요없는 컬럼 제거
        update_df = DF[['WONBU_NO', 'FINAL_JANGHAE_GRADE']]

        # db에 insert
        update_query = """UPDATE "JANGHAE_JAMUN_SKIP_PREDICT_DATA" SET "FINAL_JANGHAE_GRADE" = %s WHERE "WONBU_NO" = %s"""
        data_to_update = list(zip(update_df["FINAL_JANGHAE_GRADE"], update_df["WONBU_NO"]))
        
        with db_connect.cursor() as cur:
                extras.execute_batch(cur, update_query, data_to_update)
                db_connect.commit()

        print("최종 장해등급 산출 완료")
    
    except Exception as e:
        if db_connect:
            db_connect.rollback()
        print(f"최종 장해등급 산출 중 오류 발생: {e}")
        raise
    finally:
        if db_connect:
            db_connect.close()

############################################################################

# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}

# DAG 정의
with DAG(
    dag_id="dag_save_predict_janghae_jamun_skip_test",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    description='predict_janghae_jamun_skip_test',
    schedule_interval=None,
    catchup=False,
    tags=['predict_test']
) as dag:
    
    t1 = PythonOperator(
        task_id="start_job",
        python_callable=print_text,
        op_args=["start predict"]
    )

    t2 = PythonOperator(
        task_id="make_predict_data",
        python_callable=make_predict_data
    )

    # BashOperator로 예측 스크립트 실행
    t3 = BashOperator(
        task_id="predict_janhgae_grade_spine",
        bash_command=f"python {scripts_dir}/predict_spine.py /opt/airflow/AutogluonModels/ag-20250201_074554"
    )
    
    t4 = BashOperator(
        task_id="predict_janhgae_grade_arms",
        bash_command=f"python {scripts_dir}/predict_arms.py /opt/airflow/AutogluonModels/ag-20250203_182925"
    )

    t5 = BashOperator(
        task_id="predict_janhgae_grade_legs",
        bash_command=f"python {scripts_dir}/predict_legs.py /opt/airflow/AutogluonModels/ag-20250205_161832"
    )

    t6 = PythonOperator(
        task_id="predict_final_grade",
        python_callable=predict_final_grade
    )

    t7 = PythonOperator(
        task_id="end_job",
        python_callable=print_text,
        op_args=["end predict"]
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7