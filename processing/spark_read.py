from pyspark.sql import SparkSession

# Create a Spark session. This is your entry point to everything Spark.
spark = (
    SparkSession.builder
    .appName("ISS-Stream-Reader")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")   # keep it tiny for 8GB
    .master("local[1]")                            # 1 core, minimal footprint
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # quiet down Spark's log spam

# Read from Kafka as a streaming source
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "iss-positions")
    .option("startingOffsets", "earliest")   # read from the start of the topic
    .load()
)

# Kafka gives us data as raw bytes in a "value" column. Cast it to readable text.
messages = df.selectExpr("CAST(value AS STRING) as json_str")

# Print whatever arrives to the console
query = (
    messages.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()