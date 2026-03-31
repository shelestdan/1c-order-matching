FROM python:3.11-slim
# cache-bust: 2026-03-31-v4

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY . .

EXPOSE 8000

WORKDIR /app/scripts
ENTRYPOINT ["/bin/sh", "/app/start.sh"]
