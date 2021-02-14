import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd


spark = SparkSession.builder.appName("data-wrangling").getOrCreate()

# read the input file

path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)


# data explroation

user_log.printSchema()

user_log.describe().show()

user_log.describe("artist").show()

user_log.describe("sessionId").show()

user_log.count()

user_log.select("page").dropDuplicates().sort("page").show()

user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == "1046").collect()


# calculating statistics per hour

get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

user_log = user_log.withColumn("hour", get_hour(user_log.ts))

user_log.head()

songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderBy(user_log.hour.cast("float"))

songs_in_hour.show()

user_log_valid = user_log.dropna(how = "any", subset = ["userId", "sessionId"])

user_log_valid.count()

user_log.select("userId").dropDuplicates().sort("userId").show()

user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")

user_log_valid.count()

# users that downgrade 

user_log_valid.filter("page = 'Submit Downgrade'").show()

user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == "1138").collect()

flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

user_log_valid = user_log_valid.withColumn("downgraded", flag_downgrade_event("page"))

user_log_valid.head()

# window functions

from pyspark.sql import Window

windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)

user_log_valid = user_log_valid.withColumn("phase", Fsum("downgraded").over(windowval))

user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log.userId == "1138").sort("ts").collect()