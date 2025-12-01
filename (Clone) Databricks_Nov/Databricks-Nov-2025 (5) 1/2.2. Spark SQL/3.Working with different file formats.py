# Databricks notebook source
# MAGIC %md
# MAGIC ### Working with different file formats
# MAGIC
# MAGIC [Documentation](https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### JSON file format

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/flight-data/json

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def partitions(df):
    from pyspark.sql.functions import spark_partition_id
    df.withColumn("partition", spark_partition_id()).groupBy("partition").count().display()
    pass

# COMMAND ----------

#inputPath = "/Volumes/workspace/default/data/flight-data/json/2015-summary.json"
#inputPath = "/Volumes/workspace/default/data/flight-data/json/*-summary.json"
inputPath = "/Volumes/workspace/default/data/flight-data/json"
outputPath = "/Volumes/workspace/default/data/output/json/"

# COMMAND ----------

json_df = spark.read.json(inputPath)
json_df.display()

# COMMAND ----------

partitions(json_df)

# COMMAND ----------

json_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Apply schema on the DF**

# COMMAND ----------

my_schema = "ORIGIN_COUNTRY_NAME STRING, DEST_COUNTRY_NAME STRING, count INT"
#my_schema = "ORIGIN_COUNTRY_NAME STRING, count INT"

# COMMAND ----------

json_df_2 = spark.read.schema(my_schema).json(inputPath)
json_df_2.printSchema()

# COMMAND ----------

display(json_df_2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Working with multi-line JSON file**

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/users_multiline.json

# COMMAND ----------

multi_line_json = "/Volumes/workspace/default/data/users_multiline.json"

# COMMAND ----------

multi_line_json_df = spark.read.json(multi_line_json, multiLine=True)

display(multi_line_json_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading from nested JSON**

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/users-nested.json

# COMMAND ----------

nested_json = "/Volumes/workspace/default/data/users-nested.json"

# COMMAND ----------

nested_json_df = spark.read.json(nested_json)

display(nested_json_df)

# COMMAND ----------

nested_json_df.schema

# COMMAND ----------

#nested_json_df_2 = nested_json_df.select("userid", "name", "address.city", "address.state")
nested_json_df_2 = nested_json_df.select("userid", "name", "address.*")
display(nested_json_df_2)

# COMMAND ----------

nested_json_df_3 = nested_json_df.select("userid", explode("hobbies").alias("hobby"))

display(nested_json_df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet file format

# COMMAND ----------

parquet_file = "/Volumes/workspace/default/data/flight-data/parquet/2010-summary.parquet/"

# COMMAND ----------

parquet_df = spark.read.parquet(parquet_file)

display(parquet_df)

# COMMAND ----------

parquet_df_2 = parquet_df.where("count > 1000")
display(parquet_df_2)

# COMMAND ----------

output_path_parquet = "/Volumes/workspace/default/data/output/parquet"

# COMMAND ----------

#parquet_df_2.write.parquet(output_path_parquet)
parquet_df_2.write.parquet(output_path_parquet, compression="gzip", mode="overwrite")

# COMMAND ----------

partitions(parquet_df_2)

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/output/parquet

# COMMAND ----------

# MAGIC %md
# MAGIC #### ORC file format

# COMMAND ----------

orc_file = "/Volumes/workspace/default/data/flight-data/orc/2010-summary.orc/"

# COMMAND ----------

orc_df = spark.read.orc(orc_file)
display(orc_df)

# COMMAND ----------

orc_df_2 = orc_df.where("count > 200")
display(orc_df_2)

# COMMAND ----------

output_path_orc = "/Volumes/workspace/default/data/output/orc"

orc_df_2.write.orc(output_path_orc)

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/output/orc

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV file format
# MAGIC - Represents a delimited text file

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/flight-data/csv/

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/flight-data/csv/2010-summary.csv

# COMMAND ----------

csv_path = "/Volumes/workspace/default/data/flight-data/csv/"

# COMMAND ----------

my_schema = StructType(
    [
        StructField('ORIGIN_COUNTRY_NAME', StringType(), True), 
        StructField('DEST_COUNTRY_NAME', StringType(), True), 
        StructField('count', IntegerType(), True)
    ]
)

# COMMAND ----------

my_schema = "destination STRING, origin STRING, count INT"

# COMMAND ----------

DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count
United States,Romania,1
United States,Ireland,264

# COMMAND ----------

#csv_df = spark.read.csv(csv_path, header=True, inferSchema=True)
csv_df = spark.read.csv(csv_path, header=True, schema=my_schema)
display(csv_df)

# COMMAND ----------

csv_df.printSchema()

# COMMAND ----------

partitions(csv_df)

# COMMAND ----------

csv_df_2 = csv_df.where("count > 1000")
display(csv_df_2)

# COMMAND ----------

csv_output = "/Volumes/workspace/default/data/output/csv"

#csv_df_2.write.csv(csv_output)
#csv_df_2.write.mode('overwrite').csv(csv_output, header=True)
csv_df_2.write.mode('overwrite').csv(csv_output, header=True, sep="\t")

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/data/output/csv

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/data/output/csv/part-00000-tid-3969905049028571611-9078c4a3-2c02-434f-aa51-065634742ad1-357-1-c000.csv

# COMMAND ----------

my_schema = "destination STRING, origin STRING, count INT"
csv_df_3 = spark.read.csv(csv_output, schema=my_schema, sep="\t")

display(csv_df_3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Text file format

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/data/wordcount.txt

# COMMAND ----------

text_path = "/Volumes/workspace/default/data/wordcount.txt"

# COMMAND ----------

text_df = spark.read.text(text_path) \
            .select( explode(split("value", " ")).alias("word") ) \
            .groupBy("word").count()

display(text_df)

# COMMAND ----------

output_path_text = "/Volumes/workspace/default/data/text"
text_df.select("word").write.text(output_path_text)
