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
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/reddit"

# ------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------
spark = (
    SparkSession.builder.appName("RedditStockPipeline")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------
# UDF
# ------------------------------------------------------------
def extract_tickers_udf(title, body):
    full_text = f"{title or ''} {body or ''}".upper()
    pattern = re.compile(r"\$?([A-Z]{1,5})\b")
    return list({t for t in pattern.findall(full_text) if t in KNOWN_TICKERS_SET})


extract_tickers = udf(extract_tickers_udf, ArrayType(StringType()))

# ------------------------------------------------------------
# SCHEMAS
# ------------------------------------------------------------
stock_schema = StructType(
    [
        StructField("ticker", StringType()),
        StructField("date", StringType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
    ]
)

reddit_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("timestamp", StringType()),
        StructField("title", StringType()),
        StructField("body", StringType()),
        StructField("score", LongType()),
        StructField("num_comments", LongType()),
    ]
)

# ------------------------------------------------------------
# LOAD STOCK DATA (STATIC)
# ------------------------------------------------------------
stock_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "stock-data")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), stock_schema).alias("d"))
    .select(
        col("d.ticker"),
        to_date(col("d.date")).alias("date"),
        col("d.close"),
        col("d.volume"),
    )
    .dropna()
    .dropDuplicates(["ticker", "date"])
    .cache()
)

print("✅ Stock data loaded:", stock_df.count())

# ------------------------------------------------------------
# REDDIT STREAM
# ------------------------------------------------------------
reddit_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "reddit-data")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), reddit_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("post_date", to_date(col("event_time")))
    .withColumn("tickers", extract_tickers(col("title"), col("body")))
    .filter(size(col("tickers")) > 0)
    .withColumn("ticker", explode(col("tickers")))
    .select("id", "ticker", "post_date", "score", "num_comments")
)

# ------------------------------------------------------------
# JOIN (STREAM → STATIC)
# ------------------------------------------------------------
joined = reddit_stream.join(
    stock_df,
    (reddit_stream.ticker == stock_df.ticker)
    & (reddit_stream.post_date == stock_df.date),
    "inner",
).select(
    col("id").alias("post_id"),
    reddit_stream.ticker,
    "post_date",
    "score",
    "num_comments",
    "close",
    "volume",
    current_timestamp().alias("processed_at"),
)


# ------------------------------------------------------------
# WRITE TO MONGODB (SAFE WAY)
# ------------------------------------------------------------
def write_to_mongo(batch_df, batch_id):
    (
        batch_df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", "stockmarket_db")
        .option("collection", "processed_posts")
        .save()
    )


query = (
    joined.writeStream.foreachBatch(write_to_mongo)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="15 seconds")
    .start()
)

query.awaitTermination()
