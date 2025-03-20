from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

# DAG 정의하는 부분으로 DAG 사용시 반드시 작성해줘야 함 
with DAG(
    dag_id="dags_bash_operator", # DAG 맨 처음 화면 LIST에서 보이는 부분, 파이썬 파일명과는 상관 없음, 하지만 DAG파일명과 ID는 일치시키는게 좋음
    schedule="0 0 * * *", # 크론 스케줄: dag이 언제도는지 설정
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"), # dag이 도는 것을 언제 시작할지
    catchup=False, # start_date보다 현재 코드작성 시간이 후일때 True로 설정하면 start_time~현재시간 까지 누락된 기간이 한꺼번에 돌아감. 그래서 일반적으로 False
    #dagrun_timeout=datetime.timedelta(minutes=60), # 얼마나 돌아갈 것인지
    #tags=["example", "example2"], # list에서 보이는 tag들, optional
    #params={"example_key": "example_value"}, # dag선언 후 밑에 작성하는 task에 공통적으로 점겨주는 부분
) as dag:
    bash_t1 = BashOperator( # bash_t1은 객체명
        task_id="basg_t1", # 일반적으로 객체명과 tag_id는 동일하게 가져가는 것이 좋음
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator( 
        task_id="basg_t2",
        bash_command="echo $HOSTNAME", #$HOSTNAME: HOSTNAME이라고 하는 환경변수 값을 출력해
    )

    bash_t1 >> bash_t2