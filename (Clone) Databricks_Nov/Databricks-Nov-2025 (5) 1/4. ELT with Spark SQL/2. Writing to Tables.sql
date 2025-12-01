-- Databricks notebook source
-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Tables

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders

-- COMMAND ----------

SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DROP TABLE IF EXISTS orders;

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders/export_001.parquet`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders`


-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT * FROM orders@v0

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

RESTORE TABLE orders VERSION AS OF 0

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders`

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Appending Data

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders-new
-- MAGIC

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`dbfs:/FileStore/data/bookstore/orders-new`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

-- COMMAND ----------

-- DBTITLE 1,target
SELECT * FROM customers

-- COMMAND ----------

-- DBTITLE 1,source
SELECT * FROM customers_updates

-- COMMAND ----------

MERGE INTO customers AS T
USING customers_updates AS S
ON T.customer_id = S.customer_id
WHEN MATCHED AND S.email IS NOT NULL AND T.email IS NULL
  THEN UPDATE SET email = S.email, updated = S.updated
WHEN NOT MATCHED THEN INSERT *


-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **An other example**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

SELECT * FROM books_updates

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

MERGE INTO books AS T
USING books_updates AS S
ON T.book_id = S.book_id
WHEN NOT MATCHED AND S.category = 'Computer Science' THEN INSERT *


-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

DESC HISTORY books

-- COMMAND ----------


