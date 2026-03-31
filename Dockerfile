FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer cache)
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

# Copy project
COPY . .

# Work from scripts directory
WORKDIR /app/scripts

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "matching_api:app", "--host", "0.0.0.0", "--port", "8000"]
