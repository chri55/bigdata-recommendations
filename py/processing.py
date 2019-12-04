"""Run a linear regression using Apache Spark ML.

In the following PySpark (Spark Python API) code, we take the following actions:

  * Load a previously created linear regression (BigQuery) input table
    into our Cloud Dataproc Spark cluster as an RDD (Resilient
    Distributed Dataset)
  * Transform the RDD into a Spark Dataframe
  * Vectorize the features on which the model will be trained
  * Compute a linear regression using Spark ML

"""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, array, explode, lit, struct
# The imports, above, allow us to access SparkML features specific to linear
# regression as well as the Vectors types.


# Define a function that collects the features of interest
# (mother_age, father_age, and gestation_weeks) into a vector.
# Package the vector in a tuple containing the label (`weight_pounds`) for that
# row.
def to_long(df, by = ["reviewerID"]): # "by" is the column by which you want the final output dataframe to be grouped by

    cols = [c for c in df.columns if c not in by]

    kvs = explode(array([struct(lit(c).alias("asin"), col(c).alias("overall")) for c in cols])).alias("kvs")

    long_df = df.select(by + [kvs]).select(by + ["kvs.asin", "kvs.overall"]).filter("overall IS NOT NULL")
    # Excluding null ratings values since ALS in Pyspark doesn't want blank/null values

    return long_df


# Use Cloud Dataprocs automatically propagated configurations to get
# the Cloud Storage bucket and Google Cloud Platform project for this
# cluster.
sc = SparkContext()
spark = SparkSession(sc)
bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")

# Set an input directory for reading data from Bigquery.
input_directory = "gs://{}".format(bucket)

# Set the configuration for importing data from BigQuery.
# Specifically, make sure to set the project ID and bucket for Cloud Dataproc,
# and the project ID, dataset, and table names for BigQuery.
conf = {
    # Input Parameters
    "mapred.bq.project.id": project,
    "mapred.bq.gcs.bucket": bucket,
    "mapred.bq.temp.gcs.path": input_directory,
    "mapred.bq.input.project.id": project,
    "mapred.bq.input.dataset.id": "bookreviews_AMZ",
    "mapred.bq.input.table.id": "BookReviews_ALS_Ready",
}

# Read the data from BigQuery into Spark as an RDD.
table_data = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf)

# Extract the JSON strings from the RDD.
table_json = table_data.map(lambda x: x[1])

# Load the JSON strings as a Spark Dataframe.
bookreview_data = spark.read.json(table_json)

# Create a view so that Spark SQL queries can be run against the data.
bookreview_data.createOrReplaceTempView("bookreviews")


# As a precaution, run a query in Spark SQL to ensure no NULL values exist.
sql_query = """
SELECT *
from bookreviews
where overall is not null
and asin is not null
and reviewerID is not null
"""
clean_data = spark.sql(sql_query)

required_cols = clean_data.select(df['asin'], df['overall'], df['reviewerID'])
#required_cols.show()
indexer = [StringIndexer(inputCol = column, outputCol = column + "_index") for column in list(set(required_cols.columns) - set(['overall']))]
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(required_cols).transform(required_cols)
# Create an input DataFrame for Spark ML using the above function.
(training, test) = transformed.randomSplit([0.8, 0.2], 42)
training.cache()

als = ALS(maxIter=15, regParam=0.05, userCol = "reviewerID_index", itemCol = "asin_index", ratingCol = "overall", coldStartStrategy="drop")
als_model = als.fit(training)

predictions = als_model.transform(test)
evaluator = RegressionEvaluator(metricName = "rmse", labelCol = "overall", predictionCol = "prediction")
rmse = evaluator.evaluate(predictions)
#print("Root-mean-square error =" + str(rmse))
predictions.show()
recommendations = als_model.recommendForAllUsers(20).show(10)
return recommendations
#recsPandas = als_model.recommendForAllUsers(10).toPandas()
#mergeSeries = recsPandas.recommendations.apply(pd.Series).merge(recsPandas, right_index = True, left_index =True).drop(["recommendations"], axis = 1).melt(id_vars = ['reviewerID_index'], value_name = "recommendation").drop("variable", axis = 1).dropna()
#mergeSeries = mergeSeries.sort_values('reviewerID_index')
#mergeSeries = pd.concat([mergeSeries['recommendation'].apply(pd.Series), mergeSeries['reviewerID_index']], axis = 1)
#mergeSeries.columns = ['ProductID_index', 'Rating', 'UserID_index']

#md = transformed.select(transformed['reviewerID'], transformed['reviewerID_index'], transformed['asin'], transformed['asin_index'])
#md = md.toPandas()
#dict1 = dict(zip(md['reviewerID_index'], md['reviewerID']))
#dict2 = dict(zip(md['asin_index'], md['asin']))
#mergeSeries['reviewerID']= mergeSeries['UserID_index'].map(dict1)
#mergeSeries['asin'] = mergeSeries['ProductID_index'].map(dict2)
#mergeSeries = mergeSeries.sort_values('reviewerID')
#mergeSeries.reset_index(drop=True, inplace=True)
#new = mergeSeries[['reviewerID', 'asin', 'Rating']]
#new['recommendations'] = list(zip(new.asin, new.Rating))
#res = new[['reviewerID', 'recommendations']]
#res_new = res['recommendations'].groupby([res.reviewerID]).apply(list).reset_index()
#print(res_new)
