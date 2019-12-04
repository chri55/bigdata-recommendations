from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql.functions import col

def getRecommendations(userReviews):
    """
    Returns books based on what the user likes and dislikes

    list ratings: [("asin", "overall")]
    """

    ratingtable = spark.read.json("gs://als-output/als-preprocessed.json/*.json")

    data_to_train = []
    book = 0
    for e in userReviews:
        data_to_train.append(("asin_{}".format(book), e[0], e[1], "test_user", 2000000001))
    df = spark.createDataFrame(data_to_train, ["asin", "asin_index", "overall", "reviewerID", "reviewerID_index"])

    ratingtable = ratingtable.union(df)

    als = ALS(maxIter=5, regParam=0.01, userCol='reviewerID_index', itemCol='asin_index', ratingCol='overall', coldStartStrategy="drop")

    (training, test) = ratingtable.randomSplit([0.8, 0.2], 42)

    model = als.fit(training)
    model.transform(test)

    recs = model.recommendForUserSubset(df, 5)

    recList = recs.where(recs.reviewerID_index == 2000000001).select("recommendations").collect()

    print(recList)

    print("loading metadata")
    meta = spark.read.json("gs://metabooks/*.json")


    asins = []

    print(recList[0].asDict()["recommendations"])

    for e in recList[0].asDict()["recommendations"]:
        i = e.asDict(recursive=True)
        asins.append(ratingtable.where(ratingtable.asin_index == int(i["asin_index"])).select("asin").collect())

    print(asins)

    titles = []

    for e in asins:
        i = e[0].asDict(recursive=True)
        titles.append(meta.where(meta.asin == i["asin"]).select("title").collect())

    print(titles)





spark = (
            SparkSession.builder.master("yarn")
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
spark.conf.set('spark.sql.pivotMaxValues', u'367982')
spark.conf.set('spark.driver.memory', u'15g')
print("GET BUCKET SET SPARK CONFIGS")
l = [(10002, 5), (110661, 5), (104742, 2), (142626, 5)]
g = getRecommendations(l)
