from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark-test").getOrCreate()

# Простейшее вычисление
nums = spark.range(10)
total = nums.count()

print("Total =", total)

spark.stop()
