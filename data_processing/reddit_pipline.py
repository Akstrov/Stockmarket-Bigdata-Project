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
CHECKPOINT_BASE = "/tmp/spark-checkpoints/reddit"

# ------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------
spark = (
    SparkSession.builder.appName("RedditFeaturePipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .config("spark.sql.streaming.schemaInference", "true")  # Better schema handling
    .config("spark.sql.shuffle.partitions", "10")  # Optimize for small cluster
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# UDF: TICKER EXTRACTION (IMPROVED)
# ------------------------------------------------------------
def extract_tickers_udf(title, body):
    """Extract stock tickers, handling None values"""
    try:
        text = f"{title or ''} {body or ''}".upper()
        matches = re.findall(r"\$?([A-Z]{1,5})\b", text)
        valid = list({t for t in matches if t in KNOWN_TICKERS_SET})
        return valid if valid else None  # Return None if no tickers found
    except:
        return None


extract_tickers = udf(extract_tickers_udf, ArrayType(StringType()))

# ------------------------------------------------------------
# REDDIT SCHEMA (IMPROVED - Added nullable fields)
# ------------------------------------------------------------
reddit_schema = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("body", StringType(), nullable=True),
        StructField("score", LongType(), nullable=True),
        StructField("num_comments", LongType(), nullable=True),
        StructField("source", StringType(), nullable=True),
    ]
)

# ------------------------------------------------------------
# READ REDDIT STREAM
# ------------------------------------------------------------
print("📊 Starting Reddit stream ingestion...")

reddit_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "reddit-data")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "10000")  # Limit batch size
    .load()
    .select(from_json(col("value").cast("string"), reddit_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("tickers", extract_tickers(col("title"), col("body")))
    .filter(
        (col("tickers").isNotNull()) & (size(col("tickers")) > 0)
    )  # Better null handling
    .withColumn("ticker", explode(col("tickers")))
    .select(
        col("id").alias("post_id"),
        "ticker",
        "event_time",
        "title",
        "body",
        coalesce(col("score"), lit(0)).alias("score"),  # Handle nulls
        coalesce(col("num_comments"), lit(0)).alias("num_comments"),
    )
)

print("✅ Reddit stream configured")


# ------------------------------------------------------------
# WRITE RAW (BRONZE) - IMPROVED ERROR HANDLING
# ------------------------------------------------------------
def write_raw(batch_df, batch_id):
    """Write raw reddit data to MongoDB"""
    count = batch_df.count()

    if count == 0:
        print(f"⏭️  Batch {batch_id}: No data, skipping")
        return

    print(f"📝 Batch {batch_id}: Writing {count} rows to reddit_raw...")

    try:
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", "stockmarket_db")
            .option("collection", "reddit_raw")
            .save()
        )
        print(f"✅ Batch {batch_id}: Success!")
    except Exception as e:
        print(f"❌ Batch {batch_id}: Error - {str(e)[:100]}")


raw_query = (
    reddit_raw.writeStream.foreachBatch(write_raw)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
    .trigger(processingTime="30 seconds")  # More frequent updates
    .start()
)

print("✅ Raw data stream started")

# ------------------------------------------------------------
# FEATURE AGGREGATION (SILVER) - IMPROVED
# ------------------------------------------------------------
print("📊 Creating feature aggregations...")

reddit_features = (
    reddit_raw.withWatermark("event_time", "1 hour")  # Increased watermark
    .groupBy(
        window(
            col("event_time"), "15 minutes", "15 minutes"
        ),  # Non-overlapping windows
        col("ticker"),
    )
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        sum("score").alias("total_score"),
        avg("num_comments").alias("avg_comments"),
        max("score").alias("max_score"),
        collect_list("title").alias("sample_titles"),  # Keep some titles for analysis
    )
    .select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "post_count",
        round(col("avg_score"), 2).alias("avg_score"),
        "total_score",
        round(col("avg_comments"), 2).alias("avg_comments"),
        "max_score",
        slice(col("sample_titles"), 1, 5).alias("top_5_titles"),  # Keep top 5 titles
        current_timestamp().alias("processed_at"),
    )
)

print("✅ Feature aggregation configured")


# ------------------------------------------------------------
# WRITE FEATURES (SILVER) - IMPROVED
# ------------------------------------------------------------
def write_features(batch_df, batch_id):
    """Write aggregated features to MongoDB"""
    count = batch_df.count()

    if count == 0:
        print(f"⏭️  Feature batch {batch_id}: No data, skipping")
        return

    print(f"📊 Feature batch {batch_id}: Writing {count} aggregations...")

    # Show sample before writing
    print("Sample data:")
    batch_df.select("ticker", "window_start", "post_count", "avg_score").show(
        5, truncate=False
    )

    try:
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", "stockmarket_db")
            .option("collection", "reddit_features_15m")
            .save()
        )
        print(f"✅ Feature batch {batch_id}: Success!")
    except Exception as e:
        print(f"❌ Feature batch {batch_id}: Error - {str(e)[:100]}")


feature_query = (
    reddit_features.writeStream.foreachBatch(write_features)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/features")
    .trigger(processingTime="5 minutes")
    .start()
)

print("✅ Feature stream started")

# ------------------------------------------------------------
# MONITORING & TERMINATION
# ------------------------------------------------------------
print("\n" + "=" * 70)
print("✅ SPARK STREAMING PIPELINE RUNNING")
print("=" * 70)
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVER}")
print(f"MongoDB: {MONGO_URI}")
print(f"Tickers: {', '.join(KNOWN_TICKERS)}")
print("\nCollections:")
print("  - reddit_raw (bronze layer)")
print("  - reddit_features_15m (silver layer)")
print("\nPress Ctrl+C to stop")
print("=" * 70 + "\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping streams...")
    raw_query.stop()
    feature_query.stop()
    spark.stop()
    print("✅ Stopped gracefully")
