# Helpful resources:
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html
import findspark
findspark.init()

from pyspark.sql import SparkSession
# from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
# from pyspark.sql.functions import sum as Fsum
# from pyspark.sql.window import Window
# from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("spark-sql").getOrCreate()

user_log = spark.read.json("data/sparkify_log_small.json")

user_log.createOrReplaceTempView("log_table")

user_log.printSchema()

# QUESTION 1 - Which page did user id "" (empty string) NOT visit?

spark.sql("""
    select distinct page from log_table group by page
    minus
    select distinct page from log_table where userId = '' group by page 
""").show()


# QUESTION 2 - Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?
# 
# Both Spark SQL and Spark Data Frames are part of the Spark SQL library. Hence, they both use the Spark SQL Catalyst Optimizer to optimize queries. 
# 
# You might prefer SQL over data frames because the syntax is clearer especially for teams already experienced in SQL.
# 
# Spark data frames give you more control. You can break down your queries into smaller steps, which can make debugging easier. You can also [cache](https://unraveldata.com/to-cache-or-not-to-cache/) intermediate results or [repartition](https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4) intermediate results.

# QUESTION 3 - How many female users do we have in the data set?

spark.sql("""
    select count(distinct userid)             
    from log_table             
    where gender = 'F'
""").show()


# QUESTION 4 - How many songs were played from the most played artist?

# Here is one solution
spark.sql("""
    select artist, count(artist) as plays         
    from log_table         
    group by artist         
    order by plays desc         
    limit 1
""").show()

# Here is an alternative solution
# Get the artist play counts
play_counts = spark.sql("""
    select artist, count(artist) as plays         
    from log_table         
    group by artist
""")

# save the results in a new view
play_counts.createOrReplaceTempView("artist_counts")

# use a self join to find where the max play equals the count value
spark.sql("""
    select a2.artist, a2.plays 
    from           
    (select max(plays) as max_plays from artist_counts) as a1           
    join artist_counts as a2           
    on a1.max_plays = a2.plays           
""").show()


# QUESTION 5 - How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.

# SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
is_home = spark.sql("""
    select userId, page, ts, case when page = 'Home' then 1 else 0 end as is_home from log_table             
    where (page = 'NextSong') or (page = 'Home')             
""")

# keep the results in a new view
is_home.createOrReplaceTempView("is_home_table")

# find the cumulative sum over the is_home column
cumulative_sum = spark.sql("""
    select *, sum(is_home) over     
    (partition by userId order by ts desc rows between unbounded preceding and current row) as period     
    from is_home_table
""")

# keep the results in a view
cumulative_sum.createOrReplaceTempView("period_table")

# find the average count for NextSong
spark.sql("""
    select avg(count_results) from           
    (select count(*) as count_results 
    from period_table 
    group by userid, period, page 
    having page = 'NextSong') as counts
""").show()

