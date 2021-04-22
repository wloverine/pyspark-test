from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

sc = spark.sparkContext

data = ['hello', 'world', 'hello', 'world', 'count', 'count', 'hello']

rdd = sc.parallelize(data)
print(rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).collect())
