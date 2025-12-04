from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-pi").getOrCreate()

data = spark.range(0, 1000000)

pi = data.rdd.map(lambda row: 4.0 / (1 + (row.id % 1000) ** 2)).mean()

print(f"Approx PI = {pi}")

spark.stop()