from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- Spark session (same minimal config as before) ---
spark = (
    SparkSession.builder
    .appName("ISS-Stream-Processor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[1]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- 1. Define the schema: tell Spark the shape of our JSON ---
schema = StructType([
    StructField("satellite_id", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude_km", DoubleType(), True),
    StructField("velocity_kmh", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ingested_at", StringType(), True),
])

# --- 2. Read from Kafka (same as before) ---
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "iss-positions")
    .option("startingOffsets", "earliest")
    .load()
)

# --- 3. Parse the JSON text into real, typed columns ---
parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")   # explode the struct into top-level columns
)

# --- 4. Process: turn the Unix timestamp into a readable datetime ---
processed = parsed.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("timestamp")))
)

# --- 5. Print the processed result to console (we'll write to bronze next step) ---
query = (
    processed.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

query.awaitTermination()