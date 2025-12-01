# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def partitions(df):
    from pyspark.sql.functions import spark_partition_id
    df.withColumn("partition", spark_partition_id()).groupBy("partition").count().display()
    pass

# COMMAND ----------

input_file = "/Volumes/workspace/default/data/flight-data/json/"
df1 = spark.read.json(input_file)

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

partitions(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **select**

# COMMAND ----------

df2 = df1.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count")

display(df2)

# COMMAND ----------

#col("age") + 1
#expr("age > 18")
#df1["DEST_COUNTRY_NAME"]
df1.DEST_COUNTRY_NAME

# COMMAND ----------

df2 = df1.select(
    col("ORIGIN_COUNTRY_NAME").alias("origin"),
    expr("DEST_COUNTRY_NAME as destination"),
    expr("cast(count as int)"),
    expr("count+10 as new_count"),
    expr("count > 200 as high_frequency"),
    expr("ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME as domestic"),
    current_date().alias("date"),
    expr("'India' as country")
)

display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **where / filter**

# COMMAND ----------

#df3 = df2.where("count > 200 and DEST_COUNTRY_NAME = 'United States'")
df3 = df2.filter("count > 200 and DEST_COUNTRY_NAME = 'United States'")
display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **orderBy / sort**

# COMMAND ----------

#df3 = df2.orderBy("count", "destination")
#df3 = df2.sort("count", "destination")

df3 = df2.sort(desc("count"), asc("destination"))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC **groupBy**
# MAGIC
# MAGIC - Returns a 'pyspark.sql.group.GroupedData' object (not a DataFrame)
# MAGIC - Apply aggregation methods to return a DataFrame

# COMMAND ----------

### CONTINUE FROM HERE

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **selectExpr**

# COMMAND ----------

df2 = df1.selectExpr(
    "ORIGIN_COUNTRY_NAME as origin",
    "DEST_COUNTRY_NAME as destination",
    "cast(count as int)",
    "count+10 as new_count",
    "count > 200 as high_frequency",
    "ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME as domestic",
    "current_date() as date",
    "'India' as country"
)

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC **withColumn** & **withColumnRenamed**

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **drop**
# MAGIC - used to exclude columns in the output dataframe

# COMMAND ----------

df2.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **dropDuplicates**
# MAGIC
# MAGIC - drops duplicate rows/data

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (1, "Raju", 5),
             (3, "Raju", 5),
             (4, "Raghu", 35),
             (4, "Raghu", 35),
             (6, "Raghu", 35),
             (7, "Ravi", 70)]

users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
display(users_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **distinct**

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (1, "Raju", 5),
             (3, "Raju", 5),
             (4, "Raghu", 35),
             (4, "Raghu", 35),
             (6, "Raghu", 35),
             (7, "Ravi", 70)]

users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
display(users_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **union, intersect, subtract**

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **repartition**
# MAGIC - Is used to increase or decrease the number of partitions of the output DF
# MAGIC - Causes global shuffle

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **coalesce**
# MAGIC - Is used to only decrease the number of partitions of the output DF
# MAGIC - Causes partition merging

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Window functions**

# COMMAND ----------

data_file = "dbfs:/FileStore/data/empdata.csv"

# COMMAND ----------

csv_schema = "id INT, name STRING, dept STRING, salary INT"

# COMMAND ----------

windows_df = spark.read.csv(data_file, schema=csv_schema)

display(windows_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc, sum, max, avg, col, dense_rank, rank, row_number, to_date, round

# COMMAND ----------

window_spec = Window.partitionBy("dept")

# COMMAND ----------

window_df_2 = windows_df.withColumn("total_dept_salary", sum(col("salary")).over(window_spec)) \
                .withColumn("avg_dept_salary", round(avg(col("salary")).over(window_spec), 1)) \
                .withColumn("max_dept_salary", max(col("salary")).over(window_spec))

# COMMAND ----------

display(window_df_2)

# COMMAND ----------

window_spec_3 = Window \
    .partitionBy("dept") \
    .orderBy(col("salary")) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------

window_df_3 = windows_df.withColumn("total_salary", sum(col("salary")).over(window_spec_3)) \
                .withColumn("avg_salary", round(avg(col("salary")).over(window_spec_3), 1)) \
                .withColumn("rank", rank().over(window_spec_3)) \
                .withColumn("drank", dense_rank().over(window_spec_3)) \
                .withColumn("row_num", row_number().over(window_spec_3))

# COMMAND ----------

display(window_df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get top 3 employees with highest salary in each department**

# COMMAND ----------

window_spec = Window.partitionBy("dept").orderBy(desc("salary"))

# COMMAND ----------

top_emp_df = windows_df.withColumn("row_num", row_number().over(window_spec)) \
                .where("row_num <= 3") \
                .drop("row_num")

# COMMAND ----------

display(top_emp_df)

# COMMAND ----------


