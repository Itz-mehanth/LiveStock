# spark/Dockerfile
FROM bitnami/spark:3.4.1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_streaming.py .

CMD ["spark-submit", "--master", "local[*]", "spark_streaming.py"]
