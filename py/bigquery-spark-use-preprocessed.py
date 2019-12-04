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
            SparkSession.builder.master("yarn")
                .config("spark.io.compression.codec", "snappy")
                .config("spark.ui.enabled", "false")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "2g")
                .config("spark.memory.offHeap.enabled",True)
                .config("spark.memory.offHeap.size","2g")
                .getOrCreate()
        )

sc = spark.sparkContext
print(sc.pythonVer)
print(sc.version)

print("CREATE SPARK SESSION")
# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = spark.sparkContext._jsc.hadoopConfiguration().get(
    'fs.gs.system.bucket')
spark.conf.set('temporaryGcsBucket', bucket)
print("GET BUCKET SET SPARK CONFIGS")
# Load data from BigQuery.
print("GETTING TABLE")
required_cols = spark.read.json('gs://als-output/als-preprocessed.json/*.json')
print("GOT TABLE")
required_cols.createOrReplaceTempView('ratings')
#ratings = ratings.repartition(6)
print("CREATE PARTITIONS")
required_cols = required_cols.repartition(8)
(training, test) = required_cols.randomSplit([0.8, 0.2], 42)
print("SPLIT DATA")
als = ALS(maxIter=5, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop")
print("CREATE ALS MODEL")
als_model = als.fit(training)
print("FIT ALS MODEL")
predictions = als_model.transform(test)
print("TRANSFORM ALS MODEL")
#als_model.save("gs://als-output/model3")
#print("SAVE MODEL")
#evaluator = RegressionEvaluator(metricName="rmse", labelCol="overall", predictionCol="prediction")
#rmse = evaluator.evaluate(predictions)
#print("Root-mean-square-error =" + str(rmse))
#predictions.show()

recommendations = als_model.recommendForAllUsers(5)
print("CREATE RECOMMENDATIONS TABLE")

print("DATA WRITE START...")
# Saving the data to BigQuery
recommendations.write.json('gs://als-output/recommendations2.json')
print("FINISHED")
