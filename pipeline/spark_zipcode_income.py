import os
import boto
import pyspark
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as psf

def main():

        spark = SparkSession \
          .builder \
          .appName("Supported_Tables_Aggregations") \
          .getOrCreate()

        sc = spark.sparkContext
        sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])


        # load zipcode and income datasets from S3
        # df_review = spark.read.json("s3n://susiehuang-s3/yelp_json_all/yelp_academic_dataset_business.json")

        df_census = spark.read.format("csv").option("header", "true").load("s3n://susiehuang-s3/yelp_all_csv/census_data.csv")
        df_zipcode1 = spark.read.format("csv").option("header", "true").load("s3n://susiehuang-s3/yelp_all_csv/zipcode_county.csv")
        df_zipcode = df_zipcode1.select(col("zip_code").alias("zipcode"),col("state").alias("state_code"),col("county").alias("County"),col("city").alias("City"))

        # data transformation for income and zipcode datasets
        df1 = df_census.join(df_zipcode, (df_census.County==df_zipcode.County))

        cond = "psf.when" + ".when".join(["(psf.col('" + c + "') == psf.col('max_value'), psf.lit('" + c + "'))" for c in df1.columns if c in ['Hispanic','White','Black','Native','Asian','Pacific']])

        df2= df1.withColumn("max_value", psf.greatest(df1.Hispanic,df1.White, df1.Black, df1.Native, df1.Asian, df1.Pacific))\
            .withColumn("MAX_Racial", eval(cond))

        df3 = df2.select('zipcode','state_code','State', 'County', 'City', 'Income','max_value', 'MAX_Racial')

        # export to DB
        df3.createOrReplaceTempView("zipcode_income_table")
        output = spark.sql("SELECT * FROM zipcode_income_table")
        output.write.format('jdbc').options(
          url='jdbc:xxx://10.0.0.7/business',
          driver='com.xxx.jdbc.Driver',
          dbtable='zipcode_income',
          user='test',
          password='test').mode('append').save()


if __name__ == '__main__':
    main()