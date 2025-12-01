# Databricks notebook source
# MAGIC %md
# MAGIC #### Socket souce - Console sink
# MAGIC - Open the web-terminal and start the nc (nc -lk 9999)

# COMMAND ----------

spark.read

# COMMAND ----------

spark.readStream

# COMMAND ----------

lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .load() 

# COMMAND ----------

from pyspark.sql.functions import explode, split

# COMMAND ----------

# Split the lines into words
words = lines.select(
    # explode turns each item in an array into a separate row
    explode(
        split(lines.value, ' ')
    ).alias('word')
)

# COMMAND ----------

# Generate running word count
wordCounts = words.groupBy('word').count()

# COMMAND ----------

query = wordCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()

# COMMAND ----------

wordCounts.createOrReplaceTempView("wordcount_tmp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wordcount_tmp_vw

# COMMAND ----------


