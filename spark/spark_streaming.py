import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import psycopg2

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

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs("C:/spark_temp", exist_ok=True)

print("\n" + "="*60)
print("Spark Streaming Configuration")
print("="*60)
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Output Directory: {OUTPUT_DIR}")
print(f"Checkpoint Directory: {CHECKPOINT_DIR}")
print("="*60 + "\n")

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("StockPriceStreamingPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    .config("spark.local.dir", "C:/spark_temp") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka Schema
# -----------------------------
schema = StructType([
    StructField("symbol", StringType(), True),      # Company symbol (AAPL, GOOGL, etc.)
    StructField("price", DoubleType(), True),       # Stock price
    StructField("timestamp", DoubleType(), True)    # Unix timestamp
])

# -----------------------------
# Read from Kafka
# -----------------------------
print("📡 Connecting to Kafka...")

try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Connected to Kafka successfully")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print("\n⚠️  Make sure Kafka is running:")
    print("   1. Start Zookeeper: zookeeper-server-start.bat config/zookeeper.properties")
    print("   2. Start Kafka: kafka-server-start.bat config/server.properties")
    spark.stop()
    exit(1)

# -----------------------------
# Parse JSON from Kafka
# -----------------------------
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# -----------------------------
# Aggregate: 10-second window per symbol
# -----------------------------
agg_df = df_parsed \
    .withWatermark("timestamp", "2 minute") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("symbol")
    ).agg(
        avg("price").alias("avg_price")
    ) \
    .select(
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
    """Save aggregated batch data to PostgreSQL table `daily_stock_agg`."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        
        # Iterate over rows and upsert
        for row in batch_df.collect():  # batch_df is a Spark DataFrame
            symbol = row.symbol
            window_start = row.window_start
            window_end = row.window_end
            avg_price = float(row.avg_price)
            
            # UPSERT: update if symbol+window_start exists, else insert
            cur.execute("""
                INSERT INTO daily_stock_agg (symbol, date, avg_price)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE
                SET avg_price = EXCLUDED.avg_price
            """, (symbol, window_start, avg_price))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Batch saved to PostgreSQL: {batch_df.count()} rows")
    except Exception as e:
        print(f"❌ Failed to save batch to PostgreSQL: {e}")

def process_batch(batch_df, batch_id):
    try:
        batch_counter["count"] += 1
        
        # Check if batch has data
        if batch_df.count() == 0:
            print(f"⏭️  Batch {batch_id}: No data")
            return
        
        # Save batch to JSON
        batch_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id}")
        batch_df.coalesce(1).write.mode("overwrite").json(batch_path)

        save_batch_to_postgres(batch_df)
        
        # Get batch info
        data_count = batch_df.count()
        symbols = batch_df.select("symbol").distinct().collect()
        symbol_list = [row.symbol for row in symbols]
        
        print(f"✅ Batch {batch_id} saved: {data_count} records, Symbols: {symbol_list}")
        
        # Trim old batches (keep last MAX_FILES)
        all_batches = sorted(
            [d for d in os.listdir(OUTPUT_DIR) if d.startswith("batch_")],
            key=lambda x: int(x.split("_")[1])
        )
        
        if len(all_batches) > MAX_FILES:
            batches_to_delete = all_batches[:-MAX_FILES]
            for batch_dir in batches_to_delete:
                batch_full_path = os.path.join(OUTPUT_DIR, batch_dir)
                shutil.rmtree(batch_full_path, ignore_errors=True)
            print(f"🗑️  Deleted {len(batches_to_delete)} old batches")
        
        # Show summary every 10 batches
        if batch_counter["count"] % 10 == 0:
            print(f"\n📊 Summary: {batch_counter['count']} batches processed, {len(all_batches)} batches stored\n")
            
    except Exception as e:
        print(f"❌ Error processing batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# -----------------------------
# Start Streaming Query
# -----------------------------
print("\n🚀 Starting streaming pipeline...")
print(f"📁 Output: {OUTPUT_DIR}")
print(f"⏰ Window: 10 seconds per symbol")
print(f"💾 Max files: {MAX_FILES}")
print("\nWaiting for data from Kafka...\n")

query = agg_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="5 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n⏹️  Stopping stream...")
    query.stop()
    spark.stop()
    print("✅ Stream stopped successfully")