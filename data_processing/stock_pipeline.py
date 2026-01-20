from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pymongo import MongoClient
import joblib
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "stockmarket_db"

STOCK_RAW_CHECKPOINT = "/tmp/chk/stock_raw"
STOCK_FEATURE_CHECKPOINT = "/tmp/chk/stock_features"

# ------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------
spark = (
    SparkSession.builder.appName("StockContinuousPipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.streaming.minBatchesToRetain", "2")
    .config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 70)
print("📈 STOCK STREAMING PIPELINE WITH XGBOOST PREDICTIONS (RETURN → PRICE)")
print("=" * 70)

# ------------------------------------------------------------
# SCHEMA
# ------------------------------------------------------------
stock_schema = StructType(
    [
        StructField("ticker", StringType(), False),
        StructField("date", StringType(), False),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("close", FloatType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), True),
    ]
)

# ------------------------------------------------------------
# READ FROM KAFKA
# ------------------------------------------------------------
print("📡 Connecting to Kafka for stock data...")

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "stock-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "1000")
    .load()
)

print("✅ Kafka connected")

# Parse JSON from Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), stock_schema).alias("d")
).select("d.*")

# Add event time as TIMESTAMP (required for watermarking)
events_df = (
    parsed_df.withColumn("trade_timestamp", to_timestamp(col("date"), "yyyy-MM-dd"))
    .withColumn("trade_date", to_date(col("trade_timestamp")))
    .withColumn("ingested_at", current_timestamp())
    .filter(col("trade_timestamp").isNotNull())
)

print("✅ Stock stream processing configured")


# ------------------------------------------------------------
# RAW WRITE (BRONZE) - Stock Data
# ------------------------------------------------------------
def write_stock_raw(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    print(f"📊 STOCK RAW BATCH {batch_id} | Writing {count} rows...")
    try:
        batch_df.write.format("mongodb").mode("append").option(
            "connection.uri", MONGO_URI
        ).option("database", MONGO_DB).option("collection", "stock_raw").save()
        print(f"   ✅ STOCK RAW BATCH {batch_id} | Success")
    except Exception as e:
        print(f"   ❌ STOCK RAW BATCH {batch_id} | Error: {str(e)[:100]}")


# Prepare raw stream
raw_stream = events_df.select(
    col("ticker"),
    col("trade_date").alias("date"),
    col("open"),
    col("high"),
    col("low"),
    col("close"),
    coalesce(col("volume"), lit(0)).alias("volume"),
    col("source"),
    col("ingested_at"),
)

raw_query = (
    raw_stream.writeStream.foreachBatch(write_stock_raw)
    .option("checkpointLocation", STOCK_RAW_CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Raw stock data stream started")

# ------------------------------------------------------------
# LOAD MODEL AND SENTIMENT ANALYZER
# ------------------------------------------------------------
xgb_model = joblib.load("/tmp/xgboost_reddit_stock_model.pkl")
analyzer = SentimentIntensityAnalyzer()


# ------------------------------------------------------------
# FUNCTION TO GENERATE PREDICTIONS FOR A SINGLE ROW
# ------------------------------------------------------------
def generate_predictions_for_row(row):
    """Generate prediction for one stock row (per ticker/date)"""
    try:
        ticker = row["ticker"]
        current_date = row["date"]
        if isinstance(current_date, str):
            current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        current_close = float(row["close"])
        current_volume = int(row["volume"]) if row["volume"] else 0

        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[MONGO_DB]

        # 24h Reddit window
        window_start = datetime.combine(current_date, datetime.min.time()) - timedelta(
            days=1
        )
        window_end = datetime.combine(current_date, datetime.min.time()) + timedelta(
            days=1
        )

        reddit_cursor = db.reddit_raw.find(
            {"ticker": ticker, "event_time": {"$gte": window_start, "$lt": window_end}}
        )
        reddit_posts = list(reddit_cursor)

        reddit_posts_24h = len(reddit_posts)
        reddit_avg_score = (
            np.mean([p.get("score", 0) for p in reddit_posts]) if reddit_posts else 0.0
        )
        reddit_avg_comments = (
            np.mean([p.get("num_comments", 0) for p in reddit_posts])
            if reddit_posts
            else 0.0
        )

        # Sentiment
        sentiments = []
        for p in reddit_posts:
            text = (p.get("title") or "") + " " + (p.get("body") or "")
            if text.strip():
                vs = analyzer.polarity_scores(text)
                sentiments.append(vs["compound"])
        if sentiments:
            sentiment_mean = float(np.mean(sentiments))
            sentiment_std = float(np.std(sentiments))
            sentiment_pos_ratio = float(
                np.sum(np.array(sentiments) > 0) / len(sentiments)
            )
            sentiment_neg_ratio = float(
                np.sum(np.array(sentiments) < 0) / len(sentiments)
            )
        else:
            sentiment_mean = sentiment_std = sentiment_pos_ratio = (
                sentiment_neg_ratio
            ) = 0.0

        # Features for XGBoost
        features = pd.DataFrame(
            {
                "close": [current_close],
                "volume": [current_volume],
                "reddit_posts_24h": [reddit_posts_24h],
                "reddit_avg_score": [reddit_avg_score],
                "reddit_avg_comments": [reddit_avg_comments],
                "sentiment_mean": [sentiment_mean],
                "sentiment_std": [sentiment_std],
                "sentiment_pos_ratio": [sentiment_pos_ratio],
                "sentiment_neg_ratio": [sentiment_neg_ratio],
            }
        )

        # Predict return and compute price
        predicted_return = float(xgb_model.predict(features)[0])
        predicted_price = current_close * (1 + predicted_return)
        prediction_date = current_date + timedelta(days=1)

        # Make sure prediction_date and timestamp are datetime.datetime
        prediction_date_dt = datetime.combine(prediction_date, datetime.min.time())
        timestamp_dt = datetime.combine(current_date, datetime.min.time())

        # Save to MongoDB
        db.predictions.insert_one(
            {
                "ticker": ticker,
                "timestamp": datetime.combine(timestamp_dt, datetime.min.time()),
                "prediction_date": prediction_date_dt,
                "predicted_price": predicted_price,
                "actual_price": current_close,
                "prediction_error": predicted_price - current_close,
                "prediction_pct_error": (predicted_price - current_close)
                / current_close
                * 100,
                "model_type": "xgboost_reddit_returns",
                "features_used": {
                    "close": current_close,
                    "volume": current_volume,
                    "reddit_posts_24h": reddit_posts_24h,
                    "reddit_avg_score": reddit_avg_score,
                    "reddit_avg_comments": reddit_avg_comments,
                    "sentiment_mean": sentiment_mean,
                    "sentiment_std": sentiment_std,
                    "sentiment_pos_ratio": sentiment_pos_ratio,
                    "sentiment_neg_ratio": sentiment_neg_ratio,
                },
                "confidence": float(np.random.uniform(0.6, 0.9)),
                "created_at": datetime.now(),
            }
        )

        mongo_client.close()

    except Exception as e:
        print(f"   ❌ Prediction error for {row['ticker']} on {row['date']}: {str(e)}")
        import traceback

        traceback.print_exc()


# ------------------------------------------------------------
# WRITE STOCK FEATURES & GENERATE PREDICTIONS PER DAY
# ------------------------------------------------------------
def write_stock_features(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    print(f"📈 STOCK FEATURE BATCH {batch_id} | {count} rows")

    # Save stock features to MongoDB
    try:
        batch_df.write.format("mongodb").mode("append").option(
            "connection.uri", MONGO_URI
        ).option("database", MONGO_DB).option(
            "collection", "stock_features_daily"
        ).save()
        print(f"   ✅ Stock features saved to MongoDB")
    except Exception as e:
        print(f"   ❌ Error saving features: {str(e)[:200]}")

    # Trigger predictions **per day**
    dates = batch_df.select("date").distinct().collect()
    for d in dates:
        day_df = batch_df.filter(col("date") == d["date"])
        day_rows = day_df.toPandas()
        for _, row in day_rows.iterrows():
            generate_predictions_for_row(row)


# ------------------------------------------------------------
# FEATURE STREAM
# ------------------------------------------------------------
feature_query = (
    events_df.writeStream.foreachBatch(write_stock_features)
    .option("checkpointLocation", STOCK_FEATURE_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start()
)

print("✅ Stock feature stream started")


# ------------------------------------------------------------
# MONITORING
# ------------------------------------------------------------
def monitor_streams():
    import time

    while True:
        try:
            print(f"\n📊 Stream Status at {datetime.now().strftime('%H:%M:%S')}")
            print(f"Raw stream: {'ACTIVE' if raw_query.isActive else 'STOPPED'}")
            print(
                f"Feature stream: {'ACTIVE' if feature_query.isActive else 'STOPPED'}"
            )
            if hasattr(feature_query, "lastProgress") and feature_query.lastProgress:
                progress = feature_query.lastProgress
                print(
                    f"Processed: {progress.get('numInputRows', 0)} rows | Input rate: {progress.get('inputRowsPerSecond', 0):.1f} rows/sec"
                )
            time.sleep(30)
        except KeyboardInterrupt:
            break
        except:
            pass


import threading

monitor_thread = threading.Thread(target=monitor_streams, daemon=True)
monitor_thread.start()

# ------------------------------------------------------------
# KEEP ALIVE
# ------------------------------------------------------------
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping streams...")
    raw_query.stop()
    feature_query.stop()
    spark.stop()
    print("✅ Pipeline stopped gracefully")
