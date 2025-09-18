from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/app/data/silver/transactions")
df.select(F.min("txn_ts"), F.max("txn_ts")).show()