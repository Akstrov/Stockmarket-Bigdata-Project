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
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# UDF: TICKER EXTRACTION
# ------------------------------------------------------------
def extract_tickers_udf(title, body):
    text = f"{title or ''} {body or ''}".upper()
    matches = re.findall(r"\$?([A-Z]{1,5})\b", text)
    return list({t for t in matches if t in KNOWN_TICKERS_SET})


extract_tickers = udf(extract_tickers_udf, ArrayType(StringType()))

# ------------------------------------------------------------
# REDDIT SCHEMA
# ------------------------------------------------------------
reddit_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("timestamp", StringType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("score", LongType()),
        StructField("num_comments", LongType()),
        StructField("source", StringType()),
    ]
)

# ------------------------------------------------------------
# READ REDDIT STREAM (RAW EVENTS)
# ------------------------------------------------------------
reddit_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "reddit-data")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), reddit_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("tickers", extract_tickers(col("title"), col("body")))
    .filter(size(col("tickers")) > 0)
    .withColumn("ticker", explode(col("tickers")))
    .select(
        col("id").alias("post_id"),
        "ticker",
        "event_time",
        "title",
        "body",
        "score",
        "num_comments",
    )
)


# ------------------------------------------------------------
# WRITE RAW REDDIT DATA (BRONZE)
# ------------------------------------------------------------
def write_raw(batch_df, batch_id):
    print(f"🔥 Batch {batch_id}, rows = {batch_df.count()}")
    (
        batch_df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", "stockmarket_db")
        .option("collection", "reddit_raw")
        .save()
    )


raw_query = (
    reddit_raw.writeStream.foreachBatch(write_raw)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
    .start()
)

# ------------------------------------------------------------
# FEATURE AGGREGATION (SILVER)
# ------------------------------------------------------------
reddit_features = (
    reddit_raw.withWatermark("event_time", "30 minutes")
    .groupBy(window(col("event_time"), "15 minutes"), col("ticker"))
    .agg(
        count("*").alias("post_count"),
        avg("score").alias("avg_score"),
        sum("score").alias("total_score"),
        avg("num_comments").alias("avg_comments"),
        max("score").alias("max_score"),
    )
    .select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "post_count",
        "avg_score",
        "total_score",
        "avg_comments",
        "max_score",
        current_timestamp().alias("processed_at"),
    )
)


# ------------------------------------------------------------
# WRITE FEATURES (SILVER)
# ------------------------------------------------------------
def write_features(batch_df, batch_id):
    print(f"🔥 Batch {batch_id}, rows = {batch_df.count()}")
    (
        batch_df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", "stockmarket_db")
        .option("collection", "reddit_features_15m")
        .save()
    )


feature_query = (
    reddit_features.writeStream.foreachBatch(write_features)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/features")
    .trigger(processingTime="5 minutes")
    .start()
)

spark.streams.awaitAnyTermination()
