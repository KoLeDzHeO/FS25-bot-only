FROM python:3.10-slim

# Install system dependencies for PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Reinstall asyncpg to avoid corrupted installation
RUN pip uninstall asyncpg -y && pip install asyncpg

COPY . .

CMD ["python", "main.py"]
