# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ***text* widget**

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

dbutils.widgets.text("first_name", "Kanakaraju", "Name")

# COMMAND ----------

first_name = dbutils.widgets.get("first_name")
print(first_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ***dropdown* widget**

# COMMAND ----------

dbutils.widgets.dropdown("city", "Hyderabad", ["Hyderabad", "Bangalore", "Chennai", "Pune"], "City")

# COMMAND ----------

city = dbutils.widgets.get("city")
print(city)

# COMMAND ----------

# MAGIC %md
# MAGIC ***multiselect* widget**

# COMMAND ----------

dbutils.widgets.multiselect("languages", "Python", ["Python", "Scala", "Java", "SQL", "R"], "Languages")

# COMMAND ----------

languages = dbutils.widgets.get("languages")
print(languages)

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

output = first_name + " lives in " + city + " and knows " + languages
dbutils.notebook.exit(output)

# COMMAND ----------


