from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = (
    SparkSession.builder
    .appName("ISS-Bronze-Writer")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[1]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("satellite_id", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude_km", DoubleType(), True),
    StructField("velocity_kmh", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ingested_at", StringType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "iss-positions")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
)

# --- Write the stream to bronze as Parquet files ---
query = (
    parsed.writeStream
    .format("parquet")
    .option("path", "data/bronze/iss_positions")        # where the files land
    .option("checkpointLocation", "data/checkpoints/bronze")  # Spark's bookmark
    .outputMode("append")
    .trigger(processingTime="30 seconds")               # write a batch every 30s
    .start()
)

query.awaitTermination()