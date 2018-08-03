from pyspark.sql import SparkSession
from operator import add
import os

spark = SparkSession \
  .builder \
  .appName("LoadS3") \
  .getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

df = spark.read.json("s3n://susiehuang-s3/yelp_all_csv/yelp_academic_dataset_business.json")
df.createOrReplaceTempView("full_table")
output = spark.sql("SELECT * FROM full_table WHERE state = 'NV'")
output.write.format('jdbc').options(
  url='jdbc:mysql://10.0.0.7/business',
  driver='com.mysql.jdbc.Driver',
  dbtable='business_NV',
  user='test',
  password='test').mode('append').save()