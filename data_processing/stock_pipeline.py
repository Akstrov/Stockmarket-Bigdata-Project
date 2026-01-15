from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
MONGO_URI = "mongodb://mongodb:27017"

spark = (
    SparkSession.builder.appName("StockStreamingPipeline")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Stock schema
stock_schema = StructType(
    [
        StructField("ticker", StringType()),
        StructField("date", StringType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("volume", LongType()),
        StructField("source", StringType()),
    ]
)

# Read from Kafka
stock_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", "stock-data")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), stock_schema).alias("d"))
    .select("d.*")
    .withColumn("trade_date", to_date(col("date")))
    .withColumn("ingested_at", current_timestamp())
)


# Write to MongoDB
def write_stock_to_mongo(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.write.format("mongodb").mode("append").option(
            "database", "stockmarket_db"
        ).option("collection", "stock_raw").save()


query = (
    stock_raw.writeStream.foreachBatch(write_stock_to_mongo)
    .option("checkpointLocation", "/tmp/spark-checkpoints/stock/raw")
    .start()
)

print("📈 Stock streaming started...")
query.awaitTermination()
