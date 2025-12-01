# Databricks notebook source
def reset_bookstore_dataset():
    dbutils.fs.rm("dbfs:/FileStore/data/bookstore", True)
    dbutils.fs.cp("dbfs:/FileStore/data/bookstore_original", "dbfs:/FileStore/data/bookstore", True)

# COMMAND ----------

def print_parquet(path = "/FileStore/delta/students") :
    i = 0
    for f in dbutils.fs.ls(path):
        if ".parquet" in f.path:
            print(f.path)
            i = i+1
    print(f"\n number of parquet files = {i}")
    pass

# COMMAND ----------

def print_json(path = "/FileStore/delta/students") :
    i = 0
    for f in dbutils.fs.ls(f"{path}/_delta_log"):
        if ".json" in f.path:
            print(f.path)
            i = i+1
    print(f"\n number of json files = {i}")
    pass

# COMMAND ----------


