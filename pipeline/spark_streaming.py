import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel



spark = SparkSession.builder.appName("Spark_Streaming").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").getOrCreate()
sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# define the schema for our stream of files (with simplified columns for POC)
schema = StructType() \
  .add("business_id", StringType()) \
  .add("star", StringType()) \ 
  .add("text",IntegerType())

# Subscribe to 1 topic
inputStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "topic-pd") \
  .load() \
  .select("business_id", "text")

# transform with the new data in the stream
model = PipelineModel.load(modelpath) # simplified model in s3
scoredStream = model.transform(inputStream)
scoredStream \
  .select( \
    to_json(struct("business_id")).alias("key"),
    col("probability").cast("string").alias("value")) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "topic-newdata") \
  .outputMode("complete") \
  .start()
  .awaitTermination()