from pyspark import SparkContext
from operator import add
import os

sc = SparkContext(appName = "simple app")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

file = sc.textFile("s3n://susiehuang-s3/yelp_all_csv/test_word_txt.txt")
counts = file.flatMap(lambda line: line.split(","))\
           .map(lambda word: (word, 1))\
           .reduceByKey(lambda a, b: a + b)
res = counts.collect()

for val in res:
    print(val)