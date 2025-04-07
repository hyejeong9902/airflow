# 기본 Airflow 이미지를 사용합니다
FROM apache/airflow:2.10.5-python3.11
#FROM apache/airflow:2.10.5

RUN apt-get update && apt-get install -y libgomp1

USER airflow 
 # 로컬의 requirements.txt 파일을 컨테이너 내부로 복사합니다
COPY requirements.txt /requirements.txt
# 복사된 requirements.txt에 명시된 패키지들을 설치합니다
RUN pip install --no-cache-dir -r /requirements.txt

# 다시 airflow 유저로 돌아갑니다
#USER airflow