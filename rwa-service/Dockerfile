FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary==2.9.11 kafka-python-ng==2.2.3

COPY rwa-service.py .

CMD ["python", "rwa-service.py"]
