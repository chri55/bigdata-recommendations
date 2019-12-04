#!/usr/bin/env python

import pyspark
import sys

inputUri = sys.argv[1]

sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession().builder.master('big-data-project-259918 [*]').getOrCreate()
review_df = spark.read.json(inputUri) #should be bookreview-00.json
print("There are {} rows in bookreview-00.json.".format(review_df.count()))
review_df.saveAsTextFile("gs://big-data-project-259918/output")
