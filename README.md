# PySpark Demo

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
