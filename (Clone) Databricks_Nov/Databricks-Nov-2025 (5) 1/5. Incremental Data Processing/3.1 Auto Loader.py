# Databricks notebook source
dataset_bookstore = "dbfs:/FileStore/data/bookstore"
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

checkpoint_location = "/FileStore/checkpoints/autoloader"

# COMMAND ----------

# MAGIC %run ../Includes/copy_utils

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-streaming")
display(files)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Auto Loader

# COMMAND ----------

(
  spark
    .readStream
      .format("cloudFiles") \
      .option("cloudFiles.format", "parquet") \
      .option("cloudFiles.schemaLocation", f"{checkpoint_location}/orders_checkpoint") \
      .load(f"{dataset_bookstore}/orders-raw")
    .writeStream
      .option("checkpointLocation", f"{checkpoint_location}/orders_checkpoint")
      .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Let's look at table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Clean Up

# COMMAND ----------

# MAGIC %sql DROP TABLE orders_updates

# COMMAND ----------

#dbutils.fs.rm(f"{checkpoint_location}/orders_checkpoint", True)

# COMMAND ----------

spark.streams.active[0].stop()

# COMMAND ----------


