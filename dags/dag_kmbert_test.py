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

# t1, t3
def print_text(text):
    print(text)

# t2: KM-BERT 업데이트(TRAIN_JANGHAE_FINAL만 사용): 학습 시 필요하므로 이전 task에서 수행 필요
db_connect = psycopg2.connect(
        database="postgres",
        user="wesleyquest",
        password="Wqasdf01!",
        host="211.218.17.10",
        port="5432"
    )

def update_bert(db_connect):
    from transformers import AutoTokenizer, AutoModel # transformers=4.39.3
    
    # 저장되어 있는 bert 불러오기
    current_dir = os.path.dirname(os.path.abspath(__file__))  # dags/
    parent_dir = os.path.dirname(current_dir) # airflow/
    model_path = os.path.join(parent_dir, "km-bert_test") # C:\Users\wq240\vscode\airflow\km-bert_test
    print(f"Model loaded from: {model_path}") # test용

    tokenizer = AutoTokenizer.from_pretrained(model_path, force_download=True) 
    model = AutoModel.from_pretrained(model_path, force_download=True)
    existing_vocab = tokenizer.get_vocab() # 기존 토큰 리스트
    print(f"Tokenizer vocab size before: {len(existing_vocab)}") # 테스트용
    # TRAIN_JANGHAE_FINAL 불러오기(새로 추가한 데이터만 불러오기)
    df = pd.read_sql_query('SELECT * FROM "TRAIN_JANGHAE_FINAL" WHERE "LAST_CHANGE_ILSI"=(SELECT MAX("LAST_CHANGE_ILSI") FROM "TRAIN_JANGHAE_FINAL")', db_connect)
    # 토큰 추출 대상 컬럼 리스트
    col_list = ["SANGHAE_BUWI_CD", "SANGBYEONG_CD", "SANGBYEONG_CD_MAJOR","SANGBYEONG_CD_MIDDLE", "SANGBYEONG_CD_SMALL",
                "SUGA_CD", "EXAM_CD", "BOJOGI_CD"]
    # 추가 토큰 리스트 생성 및 vocab 추가
    def new_add_tokens(df, col):
        token_list = list(set(
                        code.strip()  
                        for row in df.loc[df[col].notnull(),col].unique()  
                        for code in row.split(',')))
        new_tokens = [token for token in token_list if token not in existing_vocab]
        return  new_tokens
    for col in col_list:
        add_token_list = new_add_tokens(df, col)
        if add_token_list: # 리스트가 비어있지 않은 경우에만 추가
            num_added_toks = tokenizer.add_tokens(add_token_list)
            print(f"{col}: {num_added_toks} tokens added.") # test 후 삭제
    # 모델 임베딩 레이어 조절 후 저장
    print(f"Tokenizer vocab size after: {len(tokenizer)}") # test후 삭제
    model.resize_token_embeddings(len(tokenizer))
    tokenizer.save_pretrained(model_path)
    model.save_pretrained(model_path)

# 기본 args 생성
default_args = {
    #'owner' : 'Hello World',
    #'email' : ['airflow@airflow.com'],
    'email_on_failure' : False,
}
# DAG 정의
with DAG(
    dag_id="dag_kmbert_test",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    #description='train model',
    #schedule="30 6 * * *",
    schedule_interval=None,
    catchup=False,
    #tags=['train']
) as dag:
    
    t1 = PythonOperator(
        task_id="start_job",
        python_callable=print_text,
        op_args=["start test"]
    )

    t2 = PythonOperator(
        task_id="update_bert",
        python_callable=update_bert,
        op_args=[db_connect]
    )
    # 수정
    t3 = PythonOperator(
        task_id="end_job",
        python_callable=print_text,
        op_args=["end test"]
    )

    t1 >> t2 >> t3