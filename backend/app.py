import os
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

# --- Configuration ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_CONFIG = {
    "host": POSTGRES_HOST, "database": "stock_db",
    "user": "postgres", "password": "1234"
}
MODEL_DIR = "/app/models/spark_ml"

# --- Flask App & Spark Session ---
app = Flask(__name__)
CORS(app)
spark = SparkSession.builder.appName("StockPredictionAPI").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# --- Helper Functions ---
def get_historical_data():
    """Fetches raw data, leaving timezone conversion to the API route."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        query = "SELECT date, symbol, avg_price FROM daily_stock_agg WHERE date >= NOW() - INTERVAL '24 hours' ORDER BY date"
        df = pd.read_sql(query, conn)
        conn.close()
        df.rename(columns={'date': 'window_end'}, inplace=True)
        return df
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return pd.DataFrame()

# ... (predict_future function remains the same) ...
def predict_future(symbol, horizon_days):
    """Predict future prices using the Spark MLlib model."""
    model_path = os.path.join(MODEL_DIR, symbol)
    if not os.path.exists(model_path): return []
    try:
        model = LinearRegressionModel.load(model_path)
        last_timestamp = int(time.time())
        future_timestamps = [last_timestamp + (i * 3600 * 24) for i in range(1, horizon_days + 1)]
        future_df = spark.createDataFrame([(ts,) for ts in future_timestamps], ["features_ts"])
        assembler = VectorAssembler(inputCols=["features_ts"], outputCol="features")
        future_features = assembler.transform(future_df)
        predictions = model.transform(future_features)
        results = predictions.select("features_ts", "prediction").collect()
        return [{"timestamp": datetime.fromtimestamp(r.features_ts).isoformat(), "predicted_price": float(r.prediction)} for r in results]
    except Exception as e:
        print(f"❌ Error during prediction for {symbol}: {e}")
        return []

# --- API Routes ---
@app.route("/api/stock_data", methods=["GET"])
def api_stock_data():
    """Serves data with timezone-aware ISO 8601 strings."""
    df = get_historical_data()
    if df.empty:
        return jsonify([])

    # --- THIS IS THE FINAL, ROBUST FIX ---
    # Manually convert naive datetimes from DB to timezone-aware UTC strings
    records = df.to_dict(orient="records")
    for record in records:
        naive_dt = record['window_end']
        # Tell Python this datetime IS UTC
        aware_dt = naive_dt.replace(tzinfo=timezone.utc)
        # Format to a proper ISO string, which will now include timezone info
        record['window_end'] = aware_dt.isoformat()
        
    return jsonify(records)

@app.route("/api/future_trends", methods=["POST"])
def api_future_trends():
    """Endpoint to get future price predictions."""
    data = request.get_json()
    symbol = data.get("symbol")
    horizon_key = data.get("horizon", "1_week")
    horizon_map = {"1_day":1, "3_days":3, "1_week":7, "1_month":30}
    horizon_days = horizon_map.get(horizon_key, 7)
    if not symbol: return jsonify({"error": "Symbol is required."}), 400
    predictions = predict_future(symbol, horizon_days)
    if not predictions: return jsonify({"message": f"Model for '{symbol}' not found."}), 404
    return jsonify({"symbol": symbol, "horizon_days": horizon_days, "predictions": predictions})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

