from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

MONGO_URI = "mongodb://mongodb:27017"
DB = "stockmarket_db"

spark = (
    SparkSession.builder.appName("BuildTrainingDataset")
    .config(
        "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------
# 1️⃣ LOAD STOCK DATA
# ------------------------------------------------
stock_df = (
    spark.read.format("mongodb")
    .option("connection.uri", MONGO_URI)
    .option("database", DB)
    .option("collection", "stock_raw")
    .load()
    .withColumn("date", to_date("trade_date"))
)

stock_daily = stock_df.groupBy("ticker", "date").agg(
    first("open").alias("open"),
    max("high").alias("high"),
    min("low").alias("low"),
    last("close").alias("close"),
    sum("volume").alias("volume"),
)

# ------------------------------------------------
# 2️⃣ LOAD REDDIT FEATURES
# ------------------------------------------------
reddit_df = (
    spark.read.format("mongodb")
    .option("connection.uri", MONGO_URI)
    .option("database", DB)
    .option("collection", "reddit_features_15m")
    .load()
    .withColumn("date", to_date("window_start"))
)

reddit_daily = reddit_df.groupBy("ticker", "date").agg(
    sum("post_count").alias("post_count"),
    avg("avg_score").alias("avg_score"),
    sum("total_score").alias("total_score"),
    avg("avg_comments").alias("avg_comments"),
    max("max_score").alias("max_score"),
)

# ------------------------------------------------
# 3️⃣ JOIN STOCK + REDDIT
# ------------------------------------------------
training_df = (
    stock_daily.join(reddit_daily, on=["ticker", "date"], how="left")
    .fillna(0)
    .orderBy("ticker", "date")
)

# ------------------------------------------------
# 4️⃣ SAVE BACK TO MONGO
# ------------------------------------------------
training_df.write.format("mongodb").mode("overwrite").option(
    "connection.uri", MONGO_URI
).option("database", DB).option("collection", "training_daily_features").save()

# ------------------------------------------------
# 5️⃣ EXPORT CSV FOR ML TEAM
# ------------------------------------------------
training_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "data/processed/training_dataset"
)

print("✅ Training dataset ready")
