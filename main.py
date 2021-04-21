from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import Row

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])

# df.show(vertical=True)
# df.printSchema()

print(df.toPandas())