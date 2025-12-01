-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Dataset relations**
-- MAGIC 1. customers.customer_id = orders.customer_id
-- MAGIC 1. orders.books = books.book_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying JSON 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM JSON.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

-- SELECT COUNT(*) FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_*.json`
SELECT COUNT(*) FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

SELECT input_file_name() source_file, customer_id, email, profile:gender 
FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`
WHERE email IS NOT NULL

-- COMMAND ----------

SELECT input_file_name() source_file, COUNT(*) as row_count
FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`
GROUP BY source_file

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying text format

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryfile.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Querying CSV 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/books-csv

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/bookstore/books-csv/export_001.csv

-- COMMAND ----------

SELECT * FROM CSV.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) USING CSV
OPTIONS (header = "true", delimiter = ";")
LOCATION '${dataset.bookstore}/books-csv'

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Limitations of Non-Delta Tables

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

UPDATE books_csv SET price = 100 WHERE category = 'Computer Science'

-- COMMAND ----------

DELETE FROM books_csv WHERE book_id = 'B07'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

DESC EXTENDED customers

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM CSV.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

SELECT * FROM books_unparsed

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
) 
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv", 
  header = "true", 
  delimiter = ";"
)

-- COMMAND ----------

SELECT * FROM books_tmp_vw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS books AS
SELECT * FROM books_tmp_vw

-- COMMAND ----------

DESC EXTENDED books

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

CREATE TABLE books_partitioned
PARTITIONED BY (category) 
AS
SELECT * FROM books_tmp_vw

-- COMMAND ----------

SELECT * FROM books_partitioned

-- COMMAND ----------

DESC EXTENDED books_partitioned

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/books_partitioned

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/books_partitioned/category=Computer%20Science/

-- COMMAND ----------


