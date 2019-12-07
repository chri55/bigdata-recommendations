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
                .config("spark.executor.memory", "10g")
                .config("spark.driver.memory", "50g")
                .config("spark.memory.offHeap.enabled",True)
                .config("spark.memory.offHeap.size","16g")
                .getOrCreate()
        )

print("CREATE SPARK SESSION")
print("GET BUCKET SET SPARK CONFIGS")
# Load data from BigQuery.
ratings = spark.read.json('gs://als-output/als-preprocessed-nonull/*.json')
print("GET TABLE")
ratings.createOrReplaceTempView('ratings')
#ratings = ratings.repartition(5)
print("CREATE PARTITIONS")

required_cols = ratings
print("GET REQ'D COLS")

#indexer = [StringIndexer(inputCol = column, outputCol = column + "_index") for column in list(set(required_cols.columns) - set(['overall', 'title']))]
#print("CREATE STRINGINDEXER")
#pipeline = Pipeline(stages=indexer)
#print("CREATE PIPELINE")
#required_cols = pipeline.fit(required_cols).transform(required_cols)
#print("FIT_TRANSFORM STRINGINDEXER")
#required_cols.show()

#print("WRITING FILE...")
#required_cols.write.json("gs://als-output/als-preprocessed-nonull")
#print("FILE WRITTEN")

(training, test) = required_cols.randomSplit([0.8, 0.2], 42)
print("SPLIT DATA")
als = ALS(maxIter=20, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop", checkpointInterval=5)
print("CREATE ALS MODEL")
als_model = als.fit(training)
print("FITTED")
print("FIT ALS MODEL")
predictions = als_model.transform(test)
print("TRANSFORM ALS MODEL")
als_model.save("gs://als-output/gg-no-re")
print("SAVE MODEL")
#evaluator = RegressionEvaluator(metricName="rmse", labelCol="overall", predictionCol="prediction")
#rmse = evaluator.evaluate(predictions)
#print("Root-mean-square-error =" + str(rmse))
#predictions.show()

#print("CREATE RECOMMENDATIONS TABLE")

#print("DATA WRITE START...")
# Saving the data to BigQuery
#recommendations.write.json('gs://als-output/recommendations.json')
#print("FINISHED")
