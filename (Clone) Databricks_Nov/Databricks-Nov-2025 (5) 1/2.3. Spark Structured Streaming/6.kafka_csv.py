# Databricks notebook source
# MAGIC %md
# MAGIC **Useful Kafka Commands**
# MAGIC - bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic t1 --property parse.key=true --property key.separator=":"

# COMMAND ----------

spark.sparkContext.setLogLevel("ERROR")     

# COMMAND ----------

spark.sparkContext.setLogLevel("ERROR")  
spark.conf.set("spark.sql.shuffle.partitions", "1")
spark.conf.set("spark.sql.streaming.checkpointLocation", "/FileStore/checkpoint/streaming/csv");

# COMMAND ----------

df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "t1") \
        .load()

# COMMAND ----------

output = df.selectExpr("CAST(key AS STRING)", 
                        "CAST(value AS STRING)", 
                        "partition", 
                        "offset")

# COMMAND ----------

query = output\
        .writeStream\
        .outputMode('append')\
        .trigger(processingTime='5 seconds') \
        .format('csv')\
        .option("header", True) \
        .option("path", "/FileStore/output/streaming/csv") \
        .start()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/streaming/csv

# COMMAND ----------

display(spark.read.csv("/FileStore/output/streaming/csv", header=True))

# COMMAND ----------


