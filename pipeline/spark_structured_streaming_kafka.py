from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession \
  .builder \
  .appName("StructuredStreamKafka") \
  .getOrCreate()

# Subscribe to topic named "topic1", Replace the xxx in .option() with your Kafka public DNS address

df_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "ec2-52-35-164-222.us-west-2.compute.amazonaws.com:9092") \
  .option("subscribe", "topic1") \
  .load()


# Start running the query that prints the running counts to the console
query = df_stream \
  .writeStream \
  .format("console") \
  .start()