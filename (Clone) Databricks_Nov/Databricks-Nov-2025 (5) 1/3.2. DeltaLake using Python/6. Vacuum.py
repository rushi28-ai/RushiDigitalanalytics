# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####vacuum - Remove files no longer referenced by a Delta table####
# MAGIC
# MAGIC You can remove files no longer referenced by a Delta table and are older than the retention threshold by running the ***vacuum*** command on the table. 
# MAGIC
# MAGIC vacuum is not triggered automatically. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * [Delta Table utility functions ducumentation](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) 

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta/students

# COMMAND ----------

from delta.tables import DeltaTable

students_delta = DeltaTable.forPath(spark, "/FileStore/delta/students")
type(students_delta)

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

#students_delta.vacuum()        # hours: 168 hours
students_delta.vacuum(0) 

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta/students

# COMMAND ----------



# COMMAND ----------


