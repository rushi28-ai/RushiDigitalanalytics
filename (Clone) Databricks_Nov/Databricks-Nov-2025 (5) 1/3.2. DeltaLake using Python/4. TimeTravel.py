# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Time Travel - Rollback a Delta Lake Table to a Previous Version with 'Restore'####
# MAGIC
# MAGIC **Delta tables allows us to rollback our data to a previous snapshot based on a version or timestamp**
# MAGIC
# MAGIC When you’re working with a plain vanilla data lake, rolling back errors can be extremely challenging, if not impossible – especially if files were deleted. The ability to undo mistakes is a huge benefit that Delta Lake offers end users. Unlike, say, a plain vanilla Parquet table, Delta Lake preserves a history of the changes you make over time, storing different versions of your data. Rolling back your Delta Lake table to a previous version with the restore command can be a great way to reverse bad data inserts or undo an operation that mutated the table in unexpected ways.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE students_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

from delta.tables import DeltaTable
students_delta = DeltaTable.forName(spark, "students")

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

students_delta.update(
    condition="student_id <= 4",
    set={"student_gender": "'Male'"}
)

# COMMAND ----------

students_delta.delete("student_id = 10")

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE TABLE students TO VERSION AS OF 3;
# MAGIC -- RESTORE TABLE students TO TIMESTAMP AS OF 'xxxxx';

# COMMAND ----------

students_delta.restoreToVersion(3)

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.restoreToTimestamp('2025-11-21 11:40:00')

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

students_delta.detail().display()
# DESC DETAIL students

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/delta/students

# COMMAND ----------


