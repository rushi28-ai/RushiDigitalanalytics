# Databricks notebook source
# MAGIC %md
# MAGIC #### Socket souce - Console sink
# MAGIC - Open the web-terminal and start the nc (nc -lk 9999)

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the output path to store the streaming output files**

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/output/streaming/csv
# MAGIC

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/output/streaming/csv

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/csv

# COMMAND ----------

# MAGIC %md
# MAGIC Create a checkpoint directory

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/checkpoint/streaming/csv

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/checkpoint/streaming/csv

# COMMAND ----------

# MAGIC %fs ls /FileStore/checkpoint/streaming/csv

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Streaming DataFrame with Socket source (localhost:9999)

# COMMAND ----------

spark.sparkContext.setLogLevel("ERROR")  
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/checkpoint/streaming/csv");

# COMMAND ----------

lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .load() 

# COMMAND ----------

from pyspark.sql.functions import explode, split, current_timestamp

# COMMAND ----------

words = lines.select(explode(split(lines.value, ' ')).alias('word')) \
            .withColumn("ts", current_timestamp())    

# COMMAND ----------

query = words \
        .select("ts", "word")  \
        .writeStream \
        .outputMode('append') \
        .trigger(processingTime='5 seconds') \
        .format('csv') \
        .option("header", True) \
        .option("sep", "|") \
        .option("path", "/FileStore/output/streaming/csv") \
        .start()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/csv

# COMMAND ----------

display(spark.read.csv("/FileStore/output/streaming/csv", header=True, sep="|"))

# COMMAND ----------

spark.read.csv("/FileStore/output/streaming/csv").count()

# COMMAND ----------


