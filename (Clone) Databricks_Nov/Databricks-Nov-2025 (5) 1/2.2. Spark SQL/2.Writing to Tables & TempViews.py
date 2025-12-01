# Databricks notebook source
# MAGIC %md
# MAGIC **LocalTempView** 
# MAGIC - Local to a specific SparkSession
# MAGIC - Created using createOrReplaceTempView command
# MAGIC - df1.createOrReplaceTempView("users")
# MAGIC
# MAGIC **GlobalTempView**
# MAGIC - Can be accessed from multiple SparkSessions within the application
# MAGIC - Tied to "global_temp" database
# MAGIC - Created using createOrReplaceGlobalTempView command
# MAGIC - df1.createOrReplaceGlobalTempView ("gusers")

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

inputData = "/Volumes/workspace/default/data/users.json"

# COMMAND ----------

df = spark.read.json(inputData)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving to a table
# MAGIC
# MAGIC - Save as table in parquet format
# MAGIC - Save as table in delta format
# MAGIC - Save as external table (use `path` option)
# MAGIC - Save as partitioned table
# MAGIC - Save as bucketted table

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

#df.write.format("parquet").bucketBy(2, "userid").saveAsTable("users_bkt")
#df.write.format("parquet").partitionBy("gender").bucketBy(2, "userid").saveAsTable("users_part_bkt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Temp View

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls /FileStore/data

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/data/users.json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC
# MAGIC - Drop all Tables & TempViews

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`dbfs:/FileStore/data/orders.csv`

# COMMAND ----------


