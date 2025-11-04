# 파이썬/api/Dockerfile.dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=Asia/Seoul

WORKDIR /app

# 의존성 설치 (레포 루트 → 하위 폴더 경로로 명시)
COPY 파이썬/api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 앱 복사 (하위 폴더 경로로 명시)
COPY 파이썬/api/api_call.py ./api_call.py

# 컨테이너 시작 커맨드
CMD ["python", "-u", "api_call.py"]
