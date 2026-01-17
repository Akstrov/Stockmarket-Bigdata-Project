from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
KNOWN_TICKERS = ["GME", "AMC", "TSLA", "AAPL", "BB", "NOK", "PLTR", "SPCE"]
KNOWN_TICKERS_SET = set(KNOWN_TICKERS)

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
MONGO_URI = "mongodb://mongodb:27017"

RAW_CHECKPOINT = "/tmp/chk/reddit_raw"
FEATURE_CHECKPOINT = "/tmp/chk/reddit_features"

# ------------------------------------------------------------
# SPARK SESSION - FIXED: Added required packages
# ------------------------------------------------------------
spark = (
    SparkSession.builder.appName("RedditContinuousPipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 70)
print("🚀 REDDIT STREAMING PIPELINE RUNNING")
print("=" * 70)

# ------------------------------------------------------------
# SCHEMA
# ------------------------------------------------------------
reddit_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
        StructField("score", LongType(), True),
        StructField("num_comments", LongType(), True),
        StructField("source", StringType(), True),
    ]
)


# ------------------------------------------------------------
# TICKER EXTRACTION - FIXED: Better regex
# ------------------------------------------------------------
def extract_tickers(title, body):
    """Extract only known stock tickers"""
    try:
        text = f"{title or ''} {body or ''}".upper()
        # Match $TICKER or standalone TICKER
        matches = re.findall(r"\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b", text)
        # Flatten tuples and filter
        all_matches = [m[0] or m[1] for m in matches]
        valid = [t for t in set(all_matches) if t in KNOWN_TICKERS_SET]
        return valid if valid else []
    except Exception as e:
        print(f"⚠️  Ticker extraction error: {e}")
        return []


extract_tickers_udf = udf(extract_tickers, ArrayType(StringType()))

# ------------------------------------------------------------
# READ FROM KAFKA
# ------------------------------------------------------------
print("📡 Connecting to Kafka...")

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "reddit-data")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

print("✅ Kafka connected")

# Parse JSON from Kafka
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), reddit_schema).alias("d")
).select("d.*")

# Add event time and extract tickers
events_df = (
    parsed_df.withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("tickers", extract_tickers_udf(col("title"), col("body")))
    .filter(col("event_time").isNotNull())
)

print("✅ Stream processing configured")


# ------------------------------------------------------------
# RAW WRITE (BRONZE) - FIXED: MongoDB options
# ------------------------------------------------------------
def write_raw(batch_df, batch_id):
    """Write raw events to MongoDB"""
    count = batch_df.count()

    if count == 0:
        print(f"⏭️  RAW BATCH {batch_id} | No data, skipping")
        return

    print(f"🟤 RAW BATCH {batch_id} | Writing {count} rows...")

    try:
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)  # FIXED: Removed "spark."
            .option("database", "stockmarket_db")
            .option("collection", "reddit_raw")
            .save()
        )
        print(f"   ✅ RAW BATCH {batch_id} | Success")
    except Exception as e:
        print(f"   ❌ RAW BATCH {batch_id} | Error: {str(e)[:100]}")


# Prepare data for raw storage
raw_stream = events_df.select(
    col("id").alias("post_id"),
    col("event_time"),
    col("title"),
    col("body"),
    coalesce(col("score"), lit(0)).alias("score"),
    coalesce(col("num_comments"), lit(0)).alias("num_comments"),
    col("tickers"),
    current_timestamp().alias("ingested_at"),
)

raw_query = (
    raw_stream.writeStream.foreachBatch(write_raw)
    .option("checkpointLocation", RAW_CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

print("✅ Raw data stream started")

# ------------------------------------------------------------
# FEATURE AGGREGATION (SILVER)
# ------------------------------------------------------------
print("📊 Setting up feature aggregation...")

# Explode tickers (one row per ticker)
exploded_df = events_df.filter(size(col("tickers")) > 0).select(
    col("event_time"),
    explode(col("tickers")).alias("ticker"),
    col("score"),
    col("num_comments"),
)

# Aggregate features by 15-minute windows
features_df = (
    exploded_df.withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "15 minutes"), col("ticker"))
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        sum("score").alias("total_score"),
        avg("num_comments").alias("avg_comments"),
        max("score").alias("max_score"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("ticker"),
        col("post_count"),
        round(col("avg_score"), 2).alias("avg_score"),
        col("total_score"),
        round(col("avg_comments"), 2).alias("avg_comments"),
        col("max_score"),
        current_timestamp().alias("processed_at"),
    )
)


def write_features(batch_df, batch_id):
    """Write aggregated features to MongoDB"""
    count = batch_df.count()

    if count == 0:
        print(f"⏭️  FEATURE BATCH {batch_id} | No data, skipping")
        return

    print(f"🟦 FEATURE BATCH {batch_id} | Writing {count} aggregations...")

    # Show sample
    print("   Sample:")
    batch_df.select("ticker", "window_start", "post_count", "avg_score").show(
        3, truncate=False
    )

    try:
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)  # FIXED
            .option("database", "stockmarket_db")
            .option("collection", "reddit_features_15m")
            .save()
        )
        print(f"   ✅ FEATURE BATCH {batch_id} | Success")
    except Exception as e:
        print(f"   ❌ FEATURE BATCH {batch_id} | Error: {str(e)[:100]}")


feature_query = (
    features_df.writeStream.foreachBatch(write_features)
    .option("checkpointLocation", FEATURE_CHECKPOINT)
    .trigger(processingTime="2 minutes")
    .start()
)

print("✅ Feature stream started")

# ------------------------------------------------------------
# MONITORING
# ------------------------------------------------------------
print("\n" + "=" * 70)
print("✅ ALL STREAMS ACTIVE")
print("=" * 70)
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVER}")
print(f"MongoDB: {MONGO_URI}")
print(f"Tracking: {', '.join(KNOWN_TICKERS)}")
print("\nCollections:")
print("  • reddit_raw (all posts with tickers)")
print("  • reddit_features_15m (15-min aggregations)")
print("\n⏳ Waiting for data... Press Ctrl+C to stop")
print("=" * 70 + "\n")

# ------------------------------------------------------------
# KEEP ALIVE
# ------------------------------------------------------------
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\n⛔ Stopping streams...")
    raw_query.stop()
    feature_query.stop()
    spark.stop()
    print("✅ Stopped gracefully")
