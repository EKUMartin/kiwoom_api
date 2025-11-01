FROM python:3.11-slim

# 시스템 타임존 맞추고(선택), 필수 유틸 설치(선택)
ENV TZ=Asia/Seoul
WORKDIR /app

# 파이썬 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 앱
COPY app.py .

# 컨테이너 시작 시 실행
CMD ["python","-u","api_call.py"]