from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from datetime import datetime, timedelta

def predict_with_spark(symbol: str, horizon_days: int):
    """
    Load a PySpark LinearRegressionModel for the given stock symbol
    and predict future prices for 'horizon_days' days.
    """
    spark = SparkSession.builder.appName("StockPredictor").getOrCreate()

    model_path = f"/path/to/spark_models/{symbol}_lr_model"
    try:
        model = LinearRegressionModel.load(model_path)
    except Exception as e:
        print(f"[ERROR] Could not load Spark model for {symbol}: {e}")
        return []

    # Create a DataFrame for future indices
    future_dates = [datetime.now() + timedelta(days=i + 1) for i in range(horizon_days)]
    future_df = spark.createDataFrame([(i + 1,) for i in range(horizon_days)], ["index"])

    # Predict using the model
    preds = model.transform(future_df).collect()

    results = [
        {"timestamp": d.isoformat(), "predicted_price": float(row.prediction)}
        for d, row in zip(future_dates, preds)
    ]

    spark.stop()
    return results
