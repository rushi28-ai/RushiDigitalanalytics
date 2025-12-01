# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Perform MERGE operation on delta lake using delta format####

# COMMAND ----------

# MAGIC %run "./1. Create Sample Data"

# COMMAND ----------

dataPath = "dbfs:/FileStore/delta/students"
dbutils.fs.rm(dataPath, True)

# COMMAND ----------

display(students1_df)

# COMMAND ----------

students1_df.write.format('delta').save(dataPath)

# COMMAND ----------

from delta.tables import DeltaTable

students_delta = DeltaTable.forPath(spark, dataPath)
type(students_delta)


# COMMAND ----------

# DBTITLE 1,Target
display(students_delta.toDF())

# COMMAND ----------

# DBTITLE 1,Source
display(students2_df)

# COMMAND ----------

help(students_delta.merge)

# COMMAND ----------

merge_condition = students_delta.alias("t") \
    .merge(
        source = students2_df.alias("s"),
        condition = "s.student_id = t.student_id"
    )

# COMMAND ----------

merge_condition

# COMMAND ----------

help(merge_condition)

# COMMAND ----------

students_delta.alias("t")\
  .merge(
    source = students2_df.alias("s"),
    condition = "s.student_id = t.student_id"
  ) \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

# In case if you want to update some of the fields for the existing data

'''
students_delta.alias("t").merge(
    students2_df.alias("s"),
    "s.student_id = t.student_id") \
  .whenMatchedUpdate(set = {
    'student_first_name': 's.student_first_name',
    'student_last_name': 's.student_last_name',
    'student_email': 's.student_email',
    'student_gender': 's.student_gender',
    'student_phone_numbers': 's.student_phone_numbers',
    'student_address': 's.student_address'
  }) \
  .whenNotMatchedInsertAll() \
  .execute()
  '''

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/delta/students

# COMMAND ----------

students_delta.toDF().display()
