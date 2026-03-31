FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer cache)
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Copy project
COPY . .

EXPOSE 8000

CMD ["sh", "-c", "cd scripts && python3 -m uvicorn matching_api:app --host 0.0.0.0 --port ${PORT:-8000}"]
