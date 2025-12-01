# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

name = "Shaktivel"
city = "Chennai"

output = dbutils.notebook.run(
    "./3. Databricks Utilities - dbutils.widgets", 
    60, 
    {"first_name": name, "city": city})
    
print(output)

# COMMAND ----------


