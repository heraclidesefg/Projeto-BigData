print("Python configurado com sucesso no VS Code!")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Teste") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


spark.range(10).show()
