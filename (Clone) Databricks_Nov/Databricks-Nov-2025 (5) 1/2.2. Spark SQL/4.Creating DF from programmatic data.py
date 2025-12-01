# Databricks notebook source
# MAGIC %md
# MAGIC **Creating DF from programmatic data**

# COMMAND ----------

listUsers = [(1, "Raju", 5),
             (2, "Ramesh", 15),
             (3, "Rajesh", 18),
             (4, "Raghu", 35),
             (5, "Ramya", 25),
             (6, "Radhika", 35),
             (7, "Ravi", 70)]

# COMMAND ----------

my_schama = "id INT, name STRING, age INT"

# COMMAND ----------

#users_df = spark.createDataFrame(listUsers)
#users_df = spark.createDataFrame(listUsers).toDF("id", "name", "age")
#users_df = spark.createDataFrame(listUsers, ["id", "name", "age"])
users_df = spark.createDataFrame(listUsers, schema=my_schama)

display(users_df)

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating DF from RDD**

# COMMAND ----------

users_rdd = sc.parallelize(listUsers)
users_rdd.collect()

# COMMAND ----------

#users_df = users_rdd.toDF()
users_df = users_rdd.toDF(["id", "name", "age"])

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating RDD from DF**

# COMMAND ----------

users_df.rdd
