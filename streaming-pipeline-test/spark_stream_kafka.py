from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingTest") \
    .master("local[*]") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

# Convert binary key and value to strings
df_clean = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

# Print the stream to console
query = df_clean.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
