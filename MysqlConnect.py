from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

prop = {'user': 'root',
        'password': 'jkl123',
        'driver': 'com.mysql.jdbc.Driver'}

url = 'jdbc:mysql://localhost:3306/ssmbuild'

# 读取表
data = spark.read.jdbc(url=url, table='books', properties=prop)
# data.rdd.foreach(lambda x:print(x))
data.select(data['bookId'],data['detail']).write.mode('append').jdbc(url=url, table='books1', properties=prop)
