# Databricks notebook source
vol_path = "/Volumes/workspace/default/data"

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls(vol_path)

# COMMAND ----------

for f in dbutils.fs.ls(vol_path):
    print(f.path)

# COMMAND ----------

for f in dbutils.fs.ls(vol_path):
    if f.name.endswith(".json"):
        print(f.path)


# COMMAND ----------

def get_files(path, type = "json"):
    files = []
    for f in dbutils.fs.ls(path):
        if f.name.endswith(type):
            files.append(f.path)
    return files


# COMMAND ----------

for f in get_files(vol_path):
    print(f)

# COMMAND ----------

for f in get_files(vol_path, ".csv"):
    print(f)

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/default/data/demo1")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/default/data/demo2")

# COMMAND ----------

def print_list(l):
    for f in l:
        print(f.path)

# COMMAND ----------

print_list(dbutils.fs.ls(vol_path))

# COMMAND ----------

print_list(dbutils.fs.ls("dbfs:/Volumes/workspace/default/data/university/"))


# COMMAND ----------

dbutils.fs.cp(
    "dbfs:/Volumes/workspace/default/data/university/department.csv",
    "dbfs:/Volumes/workspace/default/data/demo1",
)

# COMMAND ----------

dbutils.fs.cp(
    "dbfs:/Volumes/workspace/default/data/university/",
    "dbfs:/Volumes/workspace/default/data/demo1/",
    True
)

# COMMAND ----------

print_list(dbutils.fs.ls("dbfs:/Volumes/workspace/default/data/demo1/"))

# COMMAND ----------

print(dbutils.fs.head("dbfs:/Volumes/workspace/default/data/university/department.csv"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes/workspace/default/data/demo1")

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes/workspace/default/data/demo1", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/Volumes/workspace/default/data/demo2", True)

# COMMAND ----------


