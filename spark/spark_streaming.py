import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# --- Configuration ---
KAFKA_TOPIC = "stock_prices"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
MODEL_OUTPUT_DIR = "/app/models/spark_ml"
TRAINING_INTERVAL_SECONDS = 300 # 5 minutes

# --- State variable to track the last training time ---
# Using a dictionary to make it mutable inside the function
last_training_run = {"time": 0}

# --- Database Configuration ---
POSTGRES_CONFIG = {
    "host": POSTGRES_HOST, "port": "5432", "database": "stock_db",
    "user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"
}
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
POSTGRES_TABLE = "daily_stock_agg"

os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)

# --- Spark Session ---
spark = SparkSession.builder.appName("StockPriceStreamingPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --- Schema ---
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

# --- Read from Kafka ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# --- Parse and Transform Data ---
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        from_unixtime(col("data.timestamp")).cast(TimestampType()).alias("timestamp")
    )

# --- Aggregation for the dashboard (fast, 5-second window) ---
agg_df = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "5 seconds"), col("symbol")) \
    .agg(avg("price").alias("avg_price")) \
    .select(col("window.start").alias("date"), col("symbol"), col("avg_price"))

# --- Model Training Function (no changes) ---
def train_and_save_model(symbol, history_df):
    model_path = os.path.join(MODEL_OUTPUT_DIR, symbol)
    print(f"   - Training model for {symbol}...")
    history_df = history_df.withColumn("features_ts", col("date").cast("long"))
    assembler = VectorAssembler(inputCols=["features_ts"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="avg_price")
    model = lr.fit(assembler.transform(history_df))
    model.write().overwrite().save(model_path)
    print(f"   -  Model for {symbol} saved.")

# --- THE NEW, DECOUPLED BATCH PROCESSING LOGIC ---
def process_batch(batch_df, batch_id):
    row_count = batch_df.count()
    print(f"\n📦 Processing Batch {batch_id} ({time.ctime()}) with {row_count} rows")
    
    if row_count == 0:
        print("   - ⏭️ No new data in this batch.")
        return

    batch_df.cache()
    batch_df.show(5, truncate=False)

    # --- STEP 1: Always save data to PostgreSQL for the live dashboard ---
    try:
        batch_df.write.format("jdbc").option("url", POSTGRES_URL).option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_CONFIG['user']).option("password", POSTGRES_CONFIG['password']) \
            .option("driver", POSTGRES_CONFIG['driver']).mode("append").save()
        print(f"   - Saved {row_count} rows to PostgreSQL for UI.")
    except Exception as e:
        print(f"   - Failed to save batch to PostgreSQL: {e}")

    # --- STEP 2: Conditionally run the model training ---
    current_time = time.time()
    if (current_time - last_training_run["time"]) > TRAINING_INTERVAL_SECONDS:
        print("\n" + "="*60)
        print("5-minute interval reached. Starting model training cycle...")
        
        # Get all unique symbols from the entire history to ensure all models are updated
        try:
            all_symbols_df = spark.read.format("jdbc").option("url", POSTGRES_URL) \
                .option("dbtable", f"(SELECT DISTINCT symbol FROM {POSTGRES_TABLE}) as t") \
                .option("user", POSTGRES_CONFIG['user']).option("password", POSTGRES_CONFIG['password']) \
                .option("driver", POSTGRES_CONFIG['driver']).load()
            
            distinct_symbols = [row.symbol for row in all_symbols_df.collect()]
            print(f"   - Found {len(distinct_symbols)} unique symbols to train.")

            for symbol in distinct_symbols:
                historical_data = spark.read.format("jdbc").option("url", POSTGRES_URL) \
                    .option("dbtable", f"(SELECT * FROM {POSTGRES_TABLE} WHERE symbol = '{symbol}') as t") \
                    .option("user", POSTGRES_CONFIG['user']).option("password", POSTGRES_CONFIG['password']) \
                    .option("driver", POSTGRES_CONFIG['driver']).load()
                
                if historical_data.count() > 1:
                    train_and_save_model(symbol, historical_data)
                else:
                    print(f"   -  Not enough data for {symbol}.")
            
            print("Model training cycle complete.")
            print("="*60 + "\n")
            last_training_run["time"] = current_time # Reset the timer
        except Exception as e:
            print(f"   -  An error occurred during the training cycle: {e}")
    else:
        print("   - Skipping model training (not yet 5 minutes).")

    batch_df.unpersist()

# --- Start the Streaming Query with a fast trigger for the UI ---
query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

    

