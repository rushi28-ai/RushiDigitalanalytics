# Databricks notebook source
# MAGIC %md
# MAGIC ###Compaction of Delta Tables###

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Compaction is a process of merging too many small files into fewer large files.**
# MAGIC
# MAGIC * Click [here](https://docs.delta.io/latest/best-practices.html#-delta-compact-files) for the documentation related to compaction of Delta Files. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`dbfs:/FileStore/delta/students`;

# COMMAND ----------

from delta.tables import DeltaTable
students_delta = DeltaTable.forPath(spark, "dbfs:/FileStore/delta/students")
#students_delta = DeltaTable.forName(spark, "students")

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Compaction of delta table**

# COMMAND ----------

help(students_delta.optimize)

# COMMAND ----------

students_delta.optimize().executeCompaction()
#students_delta.optimize().where(....).executeCompaction()
#students_delta.optimize().executeZOrderBy(["student_id"])

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

students_delta.restoreToVersion(2)

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Compaction using repartition**

# COMMAND ----------

# MAGIC %md
# MAGIC - Even though we use overwrite, it will not delete the old files until vacuum.
# MAGIC - The reason is to provide the ability of recovery using point in time or version.
# MAGIC - We have to run vacuum to delete the old files.
# MAGIC

# COMMAND ----------

students = spark.read.format("delta").load("dbfs:/FileStore/delta/students/")

# COMMAND ----------

students.display()

# COMMAND ----------

students.rdd.getNumPartitions()

# COMMAND ----------

(
    students
    .repartition(2)
    .write
    .option("dataChange", "false")
    .format("delta")
    .mode("overwrite")
    .save("dbfs:/FileStore/delta/students/")
)

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Equivalent command in SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE students_db.students

# COMMAND ----------

#students_delta.restoreToVersion(5)
