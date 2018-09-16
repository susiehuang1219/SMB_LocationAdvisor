import pyspark
import random
import os
import string
import re
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import function as fn
from pyspark.sql import types

from pyspark.ml.feature import CountVectorizer, NGram, VectorAssembler, HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

def remove_punct(text):
    regex = re.compile('[' + re.escape(string.punctuation) + '0-9\\r\\t\\n]')
    nopunct = regex.sub(" ", text)  
    return nopunct

# 1 representing a positive review and 0 representing a negative review
def convert_rating(rating):
    if rating >=4:
        return 1
    else:
        return 0

# ML pipeline (tokenizer, countvectorizer, idf and ngrams, final model is Logistic Regression)
# skip the model evaluating/tuning part as not the focus of this project
def ml_pipeline(inputCol=["text","target"], n=3):
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    ngrams = [
        NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i))
        for i in range(1, n + 1)
    ]

    cv = [
        CountVectorizer(vocabSize=5460,inputCol="{0}_grams".format(i),
            outputCol="{0}_tf".format(i))
        for i in range(1, n + 1)
    ]
    idf = [IDF(inputCol="{0}_tf".format(i), outputCol="{0}_tfidf".format(i), minDocFreq=5) for i in range(1, n + 1)]

    assembler = [VectorAssembler(
        inputCols=["{0}_tfidf".format(i) for i in range(1, n + 1)],
        outputCol="features"
    )]
    label_stringIdx = [StringIndexer(inputCol = "target", outputCol = "label")]
    lr = LogisticRegression(maxIter = 100, regParam = 0.0001, elasticNetParam = 1.0)
    return Pipeline(stages=tokenizer + ngrams + cv + idf+ assembler + label_stringIdx+lr)


def probability_positive(probability_column):
    return float(probability_column[1])



def main():

	spark = SparkSession.builder.appName("Sentiment").config("spark.executor.cores", "6").config("spark.executor.memory", "6g").getOrCreate()

	sc = spark.sparkContext
	sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
	sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
	sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])


	review = spark.read.json("s3n://susiehuang-s3/yelp_json_all/review_test.json")


	punct_remover = udf(lambda x: remove_punct(x))
	rating_convert = udf(lambda x: convert_rating(x))
	# apply to review raw data
	review_df = review.select('id', punct_remover('text'), rating_convert('stars'))

	review_df = review_df.withColumnRenamed('<lambda>(text)', 'text')\
	                     .withColumn('target', review_df["<lambda>(stars)"].cast(IntegerType()))\
	                     .drop('<lambda>(stars)')

	(train_set, test_set) = review_df.randomSplit([0.5,0.5], seed = xxxx)

	# %%time 
	# Model fitting
	lrModel = ml_pipeline().fit(train_set)
    
    # skip the model evaluating/tuning part as not the focus of this project
    # Accuracy Score: 0.8696
    # ROC-AUC: 0.9508

	# save model to S3
	lrModel.write().overwrite().save(sc, "s3a://susiehuang-s3/model/sentiment_model/lrmodel")


	func_probability_positive = fn.udf(probability_positive, types.DoubleType())
	prediction_probability_df = lrModel.transform(test_set).\
	    withColumn('probability_positive', func_probability_positive('probability')).\
	    select('id', 'probability_positive')
	# prediction_probability_df.show()

	prediction_probability_df.createOrReplaceTempView('tmp_predictions')
	output = spark.sql("SELECT * FROM tmp_predictions")
	output.write.format('jdbc').options(
	  url='jdbc:mysql://10.0.0.7/business',
	  driver='com.mysql.jdbc.Driver',
	  dbtable='business_sentiment',
	  user='test',
	  password='test').mode('append').save()

	sc.stop()

if __name__ == '__main__':
    main()