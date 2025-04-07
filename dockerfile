# Dockerfile
FROM apache/airflow:2.10.5-python3.11

USER root

# 필수 라이브러리 설치 (autogluon은 종속성 많아서 --no-cache-dir 사용 권장)
RUN pip install --no-cache-dir autogluon==1.1.1

USER airflow