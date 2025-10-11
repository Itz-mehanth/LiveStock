import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import psycopg2
from psycopg2.extras import execute_batch

POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "stock_db",
    "user": "postgres",
    "password": "1234"
}

# -----------------------------
# Config
# -----------------------------
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
OUTPUT_DIR = os.path.abspath("spark_output")
CHECKPOINT_DIR = os.path.abspath("spark_checkpoint")
MAX_FILES = 100
TEMP_DIR = "C:/spark_temp"

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

print("\n" + "="*60)
print("Spark Streaming Configuration")
print("="*60)
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Output Dir: {OUTPUT_DIR}")
print("="*60 + "\n")

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("StockPriceStreamingPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", TEMP_DIR) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka Schema
# -----------------------------
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# -----------------------------
# Read from Kafka
# -----------------------------
print("📡 Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
print("✅ Connected to Kafka successfully")

# -----------------------------
# Parse JSON and convert timestamp
# FIX: Don't divide by 1000 if timestamp is already in seconds
# -----------------------------
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("timestamp")
    )

# Add debug output to see raw data
print("\n🔍 Debug Mode: Will print first few records...")

# -----------------------------
# Aggregate: 10-second window per symbol
# -----------------------------
agg_df = df_parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price")
    )

# -----------------------------
# Foreach Batch: Save and Trim
# -----------------------------
batch_counter = {"count": 0}

def save_batch_to_postgres(batch_df):
    rows = batch_df.collect()
    if not rows:
        return

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        sql = """
            INSERT INTO daily_stock_agg (symbol, date, avg_price)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE
            SET avg_price = EXCLUDED.avg_price
        """
        data = [(row.symbol, row.window_start, float(row.avg_price)) for row in rows]
        execute_batch(cur, sql, data)
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Batch saved to PostgreSQL: {len(rows)} rows")
    except Exception as e:
        print(f"❌ Failed to save batch to PostgreSQL: {e}")

def process_batch(batch_df, batch_id):
    batch_counter["count"] += 1
    row_count = batch_df.count()
    
    print(f"\n{'='*60}")
    print(f"📦 Processing Batch {batch_id}")
    print(f"{'='*60}")
    print(f"Row count: {row_count}")
    
    if row_count == 0:
        print(f"⏭️  Batch {batch_id}: No data received")
        return

    try:
        # Show sample data for debugging
        print("\n📊 Sample data:")
        batch_df.show(5, truncate=False)
        
        # Save batch to JSON
        batch_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id}")
        batch_df.coalesce(1).write.mode("overwrite").json(batch_path)
        print(f"💾 Saved to: {batch_path}")

        # Save to PostgreSQL
        save_batch_to_postgres(batch_df)

        # Trim old batches
        all_batches = sorted(
            [d for d in os.listdir(OUTPUT_DIR) if d.startswith("batch_")],
            key=lambda x: int(x.split("_")[1])
        )
        if len(all_batches) > MAX_FILES:
            to_delete = all_batches[:-MAX_FILES]
            for batch_dir in to_delete:
                shutil.rmtree(os.path.join(OUTPUT_DIR, batch_dir), ignore_errors=True)
            print(f"🗑️  Deleted {len(to_delete)} old batches")

        # Summary
        if batch_counter["count"] % 5 == 0:
            print(f"\n📊 Summary: {batch_counter['count']} batches processed, {len(all_batches)} batches stored\n")
    except Exception as e:
        print(f"❌ Error processing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# -----------------------------
# Start Streaming Query
# -----------------------------
print("\n🚀 Starting streaming pipeline...")
print("⏳ Waiting for data from Kafka topic...")
print("   (Make sure your Kafka producer is running!)\n")

query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="15 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Stopping stream...")
    query.stop()
    spark.stop()
    print("✅ Stream stopped successfully")