# reading and writing data with spark
# import SparkConf and SparkSession
import findspark
findspark.init()

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

# create the spark session
spark = SparkSession.builder.appName("data-io").getOrCreate()

# check the spark context
spark_context = spark.sparkContext.getConf().getAll()
print(spark_context)

in_path = "data/sparkify_log_small.json"

user_log = spark.read.json(in_path)

user_log.printSchema()
user_log.describe().show()
user_log.show(n=1)

# write results in partitioned csv files
out_path = "data/sparkify_log_small"

user_log.write.save(out_path, format="csv", header=True)

user_log_2 = spark.read.csv(out_path, header=True)

# read csv file
user_log_2.printSchema()
user_log.describe().show()
user_log_2.select("userID").show()

