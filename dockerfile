# 기본 Airflow 이미지를 사용합니다
FROM apache/airflow:2.10.5-python3.11

# 반드시 root로 전환 후 system-level 패키지 설치
USER root
RUN apt-get update && apt-get install -y libgomp1
# 이후 airflow 유저로 돌아오기
USER airflow 

# autogluon 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# PyTorch CPU 전용 버전 명시적 설치
RUN pip install --no-cache-dir torch==2.3.1 torchvision==0.18.1 torchaudio==2.3.1 --extra-index-url https://download.pytorch.org/whl/cpu
