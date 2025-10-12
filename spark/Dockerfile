# Use Bitnami Spark image
FROM apache/spark:3.4.1

# Set working directory
WORKDIR /app

# Copy the Spark script
COPY spark_streaming.py .

# Install any Python dependencies (optional, e.g., psycopg2)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set default command to run Spark submit
CMD ["spark-submit", "--master", "local[*]", "spark_streaming.py"]
