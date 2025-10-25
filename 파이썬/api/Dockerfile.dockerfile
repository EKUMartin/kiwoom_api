FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*
ENV TZ=Asia/Seoul
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8080
ENV PORT=8080
RUN useradd -ms /bin/bash appuser
USER appuser
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
