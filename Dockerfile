FROM python:3.11-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY . .

WORKDIR /app/scripts

EXPOSE 8000

ENTRYPOINT ["/bin/sh", "/app/start.sh"]
