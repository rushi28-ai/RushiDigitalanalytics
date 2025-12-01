# Databricks notebook source
# MAGIC %fs rm -r /FileStore/output/streaming/json

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/output/streaming/json

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/checkpoint/streaming/json

# COMMAND ----------

spark.sparkContext.setLogLevel("ERROR")    
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/checkpoint/streaming/json");

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

def writeToJson(df, id):
    df.write.json("/FileStore/output/streaming/json", mode="append")
    pass

# COMMAND ----------

query = df1\
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='2 seconds') \
        .foreachBatch(writeToJson) \
        .start()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/json

# COMMAND ----------

display(spark.read.json("/FileStore/output/streaming/json"))

# COMMAND ----------


