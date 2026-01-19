from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

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
print("📈 STOCK STREAMING PIPELINE WITH MOCK PREDICTIONS")
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
    .option("maxOffsetsPerTrigger", "1000")  # Increased for better throughput
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
    """Write raw stock data to MongoDB"""
    if batch_df.isEmpty():
        print(f"⏭️ STOCK RAW BATCH {batch_id} | No data, skipping")
        return

    count = batch_df.count()
    print(f"📊 STOCK RAW BATCH {batch_id} | Writing {count} rows...")

    try:
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "stock_raw")
            .save()
        )
        print(f"   ✅ STOCK RAW BATCH {batch_id} | Success")
    except Exception as e:
        print(f"   ❌ STOCK RAW BATCH {batch_id} | Error: {str(e)[:100]}")


# Prepare data for raw storage
raw_stream = events_df.select(
    col("ticker"),
    col("trade_date"),
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
    .trigger(processingTime="10 seconds")  # Reduced for faster processing
    .start()
)

print("✅ Raw stock data stream started")

# ------------------------------------------------------------
# OPTIMIZED MOCK PREDICTION GENERATION - FIXED
# ------------------------------------------------------------
print("🤖 Setting up optimized mock prediction generation...")


def generate_optimized_predictions(batch_df, batch_id):
    """Generate mock predictions efficiently using Spark operations"""
    if batch_df.isEmpty():
        print(f"⏭️ PREDICTION BATCH {batch_id} | No stock data, skipping")
        return

    count = batch_df.count()
    print(
        f"🔮 PREDICTION BATCH {batch_id} | Generating predictions for {count} records..."
    )

    try:
        # Import builtins to avoid conflict with Spark functions
        import builtins
        from pymongo import MongoClient

        # Cache the batch data
        batch_df.cache()

        # Get unique tickers and dates
        tickers_dates = batch_df.select("ticker", "date").distinct().collect()

        if not tickers_dates:
            print(f"   ⚠️ No tickers found in batch {batch_id}")
            batch_df.unpersist()
            return

        # Extract tickers and dates using Python builtins
        tickers_list = [row.ticker for row in tickers_dates]
        dates_list = [row.date for row in tickers_dates]

        # Use Python's built-in min/max (not Spark's)
        min_date = builtins.min(dates_list) if dates_list else datetime.now().date()
        max_date = builtins.max(dates_list) if dates_list else datetime.now().date()

        # Connect to MongoDB once
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[MONGO_DB]

        # Query for Reddit features in batch
        reddit_query = {
            "ticker": {"$in": tickers_list},
            "window_start": {
                "$gte": datetime.combine(
                    min_date - timedelta(days=1), datetime.min.time()
                ),
                "$lt": datetime.combine(
                    max_date + timedelta(days=1), datetime.min.time()
                ),
            },
        }

        # Fetch all reddit data at once
        reddit_cursor = db.reddit_features_15m.find(reddit_query)
        reddit_data = list(reddit_cursor)

        if reddit_data:
            # Create a lookup dictionary for reddit metrics by ticker
            import pandas as pd

            reddit_df = pd.DataFrame(reddit_data)

            # Group by ticker and calculate metrics
            reddit_metrics = {}
            for ticker in tickers_list:
                ticker_data = reddit_df[reddit_df["ticker"] == ticker]
                if not ticker_data.empty:
                    total_posts = (
                        int(ticker_data["post_count"].sum())
                        if "post_count" in ticker_data.columns
                        else 0
                    )
                    avg_sentiment = (
                        float(ticker_data["avg_score"].mean())
                        if "avg_score" in ticker_data.columns
                        else 0.0
                    )
                    reddit_metrics[ticker] = {
                        "total_posts": total_posts,
                        "avg_sentiment": avg_sentiment,
                    }
                else:
                    reddit_metrics[ticker] = {"total_posts": 0, "avg_sentiment": 0.0}
        else:
            # No reddit data found
            reddit_metrics = {
                ticker: {"total_posts": 0, "avg_sentiment": 0.0}
                for ticker in tickers_list
            }
            print(f"   ⚠️ No Reddit data found for date range {min_date} to {max_date}")

        # Convert batch to Pandas once (more efficient than per-row)
        batch_pandas = batch_df.toPandas()
        predictions = []

        # Generate predictions using vectorized operations where possible
        for _, row in batch_pandas.iterrows():
            ticker = row["ticker"]
            current_date = row["date"]
            current_close = float(row["close"])
            current_volume = int(row["volume"]) if row["volume"] else 0

            # Get reddit metrics from lookup
            reddit_info = reddit_metrics.get(
                ticker, {"total_posts": 0, "avg_sentiment": 0.0}
            )
            total_posts = reddit_info["total_posts"]
            avg_sentiment = reddit_info["avg_sentiment"]

            # Generate mock prediction using simple rules
            post_effect = builtins.min(total_posts * 0.0001, 0.05)

            sentiment_effect = 0
            if avg_sentiment > 1000:
                sentiment_effect = 0.02
            elif avg_sentiment < 100:
                sentiment_effect = -0.01

            volume_effect = 0
            if current_volume > 10000000:
                volume_effect = 0.015
            elif current_volume < 1000000:
                volume_effect = -0.01

            random_effect = float(np.random.uniform(-0.02, 0.02))

            total_effect = (
                post_effect + sentiment_effect + volume_effect + random_effect
            )
            total_effect = builtins.max(-0.10, builtins.min(0.10, total_effect))

            predicted_price = current_close * (1 + total_effect)
            prediction_date = current_date + timedelta(days=1)

            predictions.append(
                {
                    "ticker": ticker,
                    "timestamp": datetime.combine(current_date, datetime.min.time()),
                    "prediction_date": prediction_date,
                    "predicted_price": float(predicted_price),
                    "actual_price": float(current_close),
                    "prediction_error": float(predicted_price - current_close),
                    "prediction_pct_error": float(
                        (predicted_price - current_close) / current_close * 100
                    ),
                    "model_type": "mock_lstm",
                    "features_used": {
                        "current_close": float(current_close),
                        "reddit_posts_24h": int(total_posts),
                        "reddit_avg_sentiment": float(avg_sentiment),
                        "volume": int(current_volume),
                        "post_effect": float(post_effect),
                        "sentiment_effect": float(sentiment_effect),
                        "volume_effect": float(volume_effect),
                        "random_effect": float(random_effect),
                    },
                    "confidence": float(np.random.uniform(0.6, 0.9)),
                    "created_at": datetime.now(),
                }
            )

        # Write predictions in bulk
        if predictions:
            db.predictions.insert_many(predictions)
            print(f"   ✅ Generated {len(predictions)} mock predictions")

            # Show statistics
            avg_error = builtins.sum(
                p["prediction_pct_error"] for p in predictions
            ) / len(predictions)
            print(f"   📊 Average prediction error: {avg_error:.2f}%")

            # Show sample
            sample = predictions[:3]
            for p in sample:
                print(
                    f"      {p['ticker']}: ${p['actual_price']:.2f} → ${p['predicted_price']:.2f} ({p['prediction_pct_error']:+.2f}%)"
                )

        # Clean up
        batch_df.unpersist()
        mongo_client.close()

    except Exception as e:
        print(f"   ❌ Prediction generation error: {str(e)}")
        import traceback

        traceback.print_exc()


# ------------------------------------------------------------
# STOCK FEATURE PROCESSING (WITH TIMESTAMP)
# ------------------------------------------------------------
print("📊 Setting up stock feature processing...")

# Convert date to timestamp for watermarking
events_with_ts = events_df.withColumn(
    "processing_timestamp", expr("CAST(trade_timestamp AS TIMESTAMP)")
)

# Calculate daily returns and other features
features_df = (
    events_with_ts.withWatermark("processing_timestamp", "1 day")
    .groupBy(
        window(col("processing_timestamp"), "1 day").alias("time_window"), col("ticker")
    )
    .agg(
        first("open").alias("open"),
        max("high").alias("high"),
        min("low").alias("low"),
        last("close").alias("close"),
        sum("volume").alias("volume"),
        count("*").alias("trade_count"),
    )
    .select(
        col("time_window.start").alias("date"),
        col("ticker"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
        col("trade_count"),
        current_timestamp().alias("processed_at"),
    )
)


def write_stock_features(batch_df, batch_id):
    """Write stock features and generate predictions"""
    if batch_df.isEmpty():
        print(f"⏭️ STOCK FEATURE BATCH {batch_id} | No data, skipping")
        return

    count = batch_df.count()
    print(f"📈 STOCK FEATURE BATCH {batch_id} | Processing {count} stock records...")

    # Show sample (limit to 3 for performance)
    sample = batch_df.limit(3).collect()
    if sample:
        print("   Sample stock data:")
        for row in sample:
            print(
                f"     {row.ticker} | Date: {row.date} | Close: ${row.close:.2f} | Volume: {row.volume:,}"
            )

    try:
        # Write stock features to MongoDB
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "stock_features_daily")
            .save()
        )
        print(f"   ✅ Stock features saved to MongoDB")

        # Generate predictions in parallel (non-blocking)
        # For better performance, consider using a separate thread pool
        generate_optimized_predictions(batch_df, batch_id)

    except Exception as e:
        print(f"   ❌ FEATURE BATCH {batch_id} | Error: {str(e)[:200]}")


# ------------------------------------------------------------
# ALTERNATIVE: SEPARATE PREDICTION STREAM
# ------------------------------------------------------------
# For better performance, you might want to separate the prediction generation
# into its own stream that reads from stock_features_daily

feature_query = (
    features_df.writeStream.foreachBatch(write_stock_features)
    .option("checkpointLocation", STOCK_FEATURE_CHECKPOINT)
    .trigger(processingTime="30 seconds")  # Adjust based on your needs
    .outputMode("append")
    .start()
)

print("✅ Stock feature stream started")

# ------------------------------------------------------------
# MONITORING AND PROGRESS INDICATORS
# ------------------------------------------------------------
print("\n" + "=" * 70)
print("✅ ALL STOCK STREAMS ACTIVE")
print("=" * 70)
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVER}")
print(f"MongoDB: {MONGO_URI}")
print(f"Collections updated:")
print("  • stock_raw (raw stock data)")
print("  • stock_features_daily (daily aggregates)")
print("  • predictions (mock predictions)")
print("\n⏳ Waiting for stock data... Press Ctrl+C to stop")
print("=" * 70 + "\n")


# Function to monitor stream status
def monitor_streams():
    import time

    while True:
        try:
            print(f"\n📊 Stream Status at {datetime.now().strftime('%H:%M:%S')}")
            print(f"Raw stream: {'ACTIVE' if raw_query.isActive else 'STOPPED'}")
            print(
                f"Feature stream: {'ACTIVE' if feature_query.isActive else 'STOPPED'}"
            )

            # Show recent progress if available
            if hasattr(feature_query, "lastProgress"):
                progress = feature_query.lastProgress
                if progress:
                    print(f"Processed: {progress.get('numInputRows', 0)} rows")
                    print(
                        f"Input rate: {progress.get('inputRowsPerSecond', 0):.1f} rows/sec"
                    )

            time.sleep(30)  # Check every 30 seconds

        except KeyboardInterrupt:
            break
        except:
            pass


# Start monitoring in background
import threading

monitor_thread = threading.Thread(target=monitor_streams, daemon=True)
monitor_thread.start()

# ------------------------------------------------------------
# KEEP ALIVE
# ------------------------------------------------------------
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n⛔ Stopping stock streams...")
    raw_query.stop()
    feature_query.stop()
    spark.stop()
    print("✅ Stock pipeline stopped gracefully")
