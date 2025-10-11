import os
import glob
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import psycopg2
from joblib import dump, load
from sklearn.linear_model import LinearRegression
import schedule

# -----------------------------
# Config
# -----------------------------
SPARK_OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../spark_output"))
MODEL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "models"))
POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "stock_db",
    "user": "postgres",
    "password": "postgres"
}
TRAIN_INTERVAL_MINUTES = 5
os.makedirs(MODEL_DIR, exist_ok=True)

# -----------------------------
# Flask setup
# -----------------------------
app = Flask(__name__)
CORS(app)

# -----------------------------
# Helper Functions
# -----------------------------

def get_latest_batch():
    """Read the latest Spark output JSON files."""
    files = sorted(glob.glob(os.path.join(SPARK_OUTPUT_DIR, "**/*.json"), recursive=True))
    files = [f for f in files if os.path.getsize(f) > 0]
    if not files:
        return pd.DataFrame()

    df_list = [pd.read_json(f, lines=True) for f in files]
    df = pd.concat(df_list, ignore_index=True)

    if "window" in df.columns:
        df["window_start"] = df["window"].apply(lambda w: w["start"] if isinstance(w, dict) else None)
        df["window_end"] = df["window"].apply(lambda w: w["end"] if isinstance(w, dict) else None)
        df = df.drop(columns=["window"])
    return df


def get_historical_data(symbol):
    """Fetch historical aggregated stock data from PostgreSQL."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        query = f"SELECT date, avg_price FROM daily_stock_agg WHERE symbol='{symbol}' ORDER BY date"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print("Database error:", e)
        return pd.DataFrame()


def train_and_save_model(symbol):
    """Train a simple Linear Regression model for a given symbol."""
    latest_df = get_latest_batch()
    hist_df = get_historical_data(symbol)

    latest_symbol_df = (
        latest_df[latest_df.symbol == symbol][["avg_price"]]
        if not latest_df.empty and "symbol" in latest_df.columns
        else pd.DataFrame()
    )

    combined_df = pd.concat([hist_df, latest_symbol_df], ignore_index=True).reset_index(drop=True)

    if combined_df.empty:
        print(f"No data to train model for {symbol}")
        return None

    X = [[i + 1] for i in range(len(combined_df))]
    y = combined_df["avg_price"].values
    model = LinearRegression()
    model.fit(X, y)

    model_path = os.path.join(MODEL_DIR, f"{symbol}_model.joblib")
    dump(model, model_path)
    print(f"Model trained and saved for {symbol}: {model_path}")
    return model


def predict_future(model, last_date, horizon_days):
    """Generate simple linear predictions for the next N days."""
    if model is None:
        return []

    days = max(1, int(round(horizon_days)))
    future_dates = [last_date + timedelta(days=i) for i in range(1, days + 1)]
    X_future = [[i] for i in range(1, days + 1)]
    preds = model.predict(X_future)

    return [
        {"timestamp": d.isoformat(), "predicted_price": float(p)}
        for d, p in zip(future_dates, preds)
    ]

# -----------------------------
# Background retraining scheduler
# -----------------------------
def retrain_all_models():
    latest_df = get_latest_batch()
    if latest_df.empty:
        return
    for symbol in latest_df["symbol"].unique():
        train_and_save_model(symbol)

def run_scheduler():
    schedule.every(TRAIN_INTERVAL_MINUTES).minutes.do(retrain_all_models)
    while True:
        schedule.run_pending()
        time.sleep(1)

threading.Thread(target=run_scheduler, daemon=True).start()

# -----------------------------
# Flask Routes
# -----------------------------
@app.route("/")
def home():
    return jsonify({
        "status": "Flask API is running",
        "endpoints": ["/api/stock_data", "/api/future_trends"],
        "spark_dir": SPARK_OUTPUT_DIR
    })

@app.route("/api/stock_data", methods=["GET"])
def api_stock_data():
    df = get_latest_batch()
    return jsonify(df.to_dict(orient="records")), 200


@app.route("/api/future_trends", methods=["POST"])
def api_future_trends():
    """
    Predict future stock prices for a given symbol and time horizon.
    Uses an existing trained model if available; otherwise trains a new one.
    Falls back to sklearn if PySpark is not available.
    """
    try:
        data = request.get_json(force=True)
        symbol = data.get("symbol")
        horizon_key = data.get("horizon", "1_week")

        if not symbol:
            return jsonify({"error": "Missing required field: symbol"}), 400

        horizon_map = {
            "1_hour": 1 / 24,
            "6_hours": 6 / 24,
            "12_hours": 12 / 24,
            "1_day": 1,
            "3_days": 3,
            "1_week": 7,
            "1_month": 30,
            "3_months": 90,
            "6_months": 180,
            "1_year": 365
        }
        horizon = horizon_map.get(horizon_key, 7)

        # Load or train model
        model_path = os.path.join(MODEL_DIR, f"{symbol}_model.joblib")
        model = load(model_path) if os.path.exists(model_path) else train_and_save_model(symbol)
        if model is None:
            return jsonify({
                "symbol": symbol,
                "message": "No data found to train model for this symbol"
            }), 404

        # Try Spark-based prediction first
        try:
            from spark.predict import predict_with_spark
            preds = predict_with_spark(symbol, int(horizon))
            if not preds:
                raise ValueError("Empty Spark predictions, using fallback.")
        except Exception as e:
            print(f"[WARN] Spark prediction failed: {e}. Falling back to sklearn.")
            preds = predict_future(model, datetime.now(), int(horizon))

        return jsonify({
            "symbol": symbol,
            "horizon": horizon_key,
            "prediction_count": len(preds),
            "predictions": preds
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# Startup
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
