FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_hdfs.py .

CMD ["python", "-u", "ingest_hdfs.py"]
