# Databricks notebook source
# MAGIC %fs rm -r /FileStore/output/streaming/parquet

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/output/streaming/parquet

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/parquet

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/checkpoint/streaming/parquet

# COMMAND ----------

spark.sparkContext.setLogLevel("ERROR")    
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/checkpoint/streaming/parquet");

# COMMAND ----------

df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df1 = df.withColumn("value2", df.value * 10) \
        .withColumn("source", lit("Rate"))

# COMMAND ----------

query = df1\
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='2 seconds') \
        .format('parquet') \
        .option("path", "/FileStore/output/streaming/parquet") \
        .start()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/parquet

# COMMAND ----------

display(spark.read.parquet("/FileStore/output/streaming/parquet"))

# COMMAND ----------


