#!/usr/bin/python
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

print("IMPORT REQ")

spark = (
            SparkSession.builder.master("local")
                .config("spark.io.compression.codec", "snappy")
                .config("spark.ui.enabled", "false")
                .config("spark.executor.memory", "16g")
                .config("spark.driver.memory", "16g")
                .config("spark.memory.offHeap.enabled",True)
                .config("spark.memory.offHeap.size","16g")
                .getOrCreate()
        )

sc = SparkContext("gcp", "Recommender App")

print("CREATE SPARK SESSION")
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)
spark.conf.set('spark.sql.pivotMaxValues', u'367982')
spark.conf.set('spark.driver.memory', u'15g')
print("GET BUCKET SET SPARK CONFIGS")
# Load data from BigQuery.
ratings = spark.read.json('gs://als-output/als-ready-data.json')
print("GET TABLE")
ratings.createOrReplaceTempView('ratings')
#ratings = ratings.repartition(6)
print("CREATE PARTITIONS")

required_cols = ratings.select(ratings['reviewerID'], ratings['asin'], ratings['overall'])
required_cols = required_cols.repartition(8)
print("GET REQ'D COLS")

indexer = [StringIndexer(inputCol = column, outputCol = column + "_index") for column in list(set(required_cols.columns) - set(['overall']))]
print("CREATE STRINGINDEXER")
pipeline = Pipeline(stages=indexer)
print("CREATE PIPELINE")
required_cols = pipeline.fit(required_cols).transform(required_cols)
print("FIT_TRANSFORM STRINGINDEXER")
required_cols.show()
print("WRITING FILE...")
required_cols.write.json("gs://als-output/als-preprocessed.json")
print("FILE WRITTEN")
(training, test) = required_cols.randomSplit([0.8, 0.2], 42)
print("SPLIT DATA")
als = ALS(maxIter=5, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop")
print("CREATE ALS MODEL")
als_model = als.fit(training)
print("FIT ALS MODEL")
predictions = als_model.transform(test)
print("TRANSFORM ALS MODEL")
sc = spark.SparkContext
als_model.save(sc, "gs://als-output/")
print("SAVE MODEL")
#evaluator = RegressionEvaluator(metricName="rmse", labelCol="overall", predictionCol="prediction")
#rmse = evaluator.evaluate(predictions)
#print("Root-mean-square-error =" + str(rmse))
#predictions.show()

recommendations = als_model.recommendProductsForUsers(5)
print("CREATE RECOMMENDATIONS TABLE")

print("DATA WRITE START...")
# Saving the data to BigQuery
recommendations.write.json('gs://als-output/recommendations.json')
print("FINISHED")
