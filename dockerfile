FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Make sure scripts are executable
RUN chmod +x /app/src/*.py

# Default command (to be overridden in docker-compose)
CMD ["python3", "-u", "/app/src/data_generator.py"]
