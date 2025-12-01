# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

dataset_bookstore = "abfss://bookstore-landing@demoykrucdemodbsa.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %run ./Includes/copy_utils

# COMMAND ----------

load_new_json_data()

# COMMAND ----------


