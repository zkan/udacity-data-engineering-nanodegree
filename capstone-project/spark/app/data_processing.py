from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Data Processing") \
    .getOrCreate()

print(spark)
