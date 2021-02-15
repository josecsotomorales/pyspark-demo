# PySpark Demo Scripts

## Install Spark
Install Spark with Homebrew

    brew install apache-spark

on OS X, the install location should be `/usr/local/opt/apache-spark/libexec`.

PySpark isn't on sys.path by default, `findspark` will ad it at runtime.

To initialize PySpark, just call

```python
import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext(appName="myAppName")
```
## Why might you prefer to use SQL over data frames?
Both Spark SQL and Spark Data Frames are part of the Spark SQL library. Hence, they both use the Spark SQL Catalyst Optimizer to optimize queries. 
You might prefer SQL over data frames because the syntax is clearer especially for teams already experienced in SQL.
Spark data frames give you more control. You can break down your queries into smaller steps, which can make debugging easier. You can also [cache](https://unraveldata.com/to-cache-or-not-to-cache/) intermediate results or [repartition](https://hackernoon.com/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4) intermediate results.
