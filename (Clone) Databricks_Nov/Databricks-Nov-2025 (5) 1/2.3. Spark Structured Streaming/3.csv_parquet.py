# Databricks notebook source
# MAGIC %fs rm -r /FileStore/input/streaming/csv

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/input/streaming/csv

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/output/streaming/parquet

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/checkpoint/streaming/parquet

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/output/streaming/parquet

# COMMAND ----------

from pyspark.sql.functions import explode, split, current_timestamp, window, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

mySchema = StructType([
            StructField("DEST_COUNTRY_NAME", StringType(), True),
            StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
            StructField("count", IntegerType(), True)])


# COMMAND ----------

    spark.sparkContext.setLogLevel("ERROR")  
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/checkpoint/streaming/parquet")

# COMMAND ----------

df = spark \
    .readStream \
    .format("csv") \
    .load("/FileStore/input/streaming/csv", header=True, schema = mySchema)  

# COMMAND ----------

df2 = df \
    .withColumn("today", current_date())\
    .withColumn("source", lit("CSV Files"))

# COMMAND ----------

query = df2 \
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='3 seconds') \
        .format('parquet') \
        .option("path", "/FileStore/output/streaming/parquet") \
        .start()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/parquet

# COMMAND ----------

display(spark.read.parquet("/FileStore/output/streaming/parquet"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/data/flight-data/csv/

# COMMAND ----------

# MAGIC %fs cp dbfs:/FileStore/data/flight-data/csv/2012_summary.csv /FileStore/input/streaming/csv

# COMMAND ----------


