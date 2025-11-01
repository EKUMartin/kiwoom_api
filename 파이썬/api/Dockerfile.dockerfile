FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 TZ=Asia/Seoul
WORKDIR /app

# 의존성 설치
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 앱 복사
COPY api_call.py ./api_call.py

# 컨테이너 시작 커맨드
CMD ["python", "-u", "api_call.py"]
