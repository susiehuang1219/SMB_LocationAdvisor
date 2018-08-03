from pyspark import SparkContext
from operator import add

sc.stop()
sc = SparkContext(appName = "simple app")

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# text_file = sc.textFile("s3n://susiehuang-s3/yelp_all_csv/yelp_business.csv")

# Loads RDD
lines = sc.textFile("s3://susiehuang-s3/yelp_all_csv/yelp_business.csv")
# Split lines into columns; change split() argument depending on deliminiter e.g. '\t'
parts = lines.map(lambda l: l.split(','))
# Convert RDD into DataFrame
df = spark.createDataFrame(parts, ['business_id','name','neighborhood','address','city','state','zipcode','latitude','longitude','stars','review_ct','is_open','category'])