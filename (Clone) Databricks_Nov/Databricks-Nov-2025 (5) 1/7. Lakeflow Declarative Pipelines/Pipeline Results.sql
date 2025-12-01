-- Databricks notebook source
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://bookstore@demoykrucdemodbsa.dfs.core.windows.net/"))

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

USE CATALOG bookstore_catalog;
USE default;

-- COMMAND ----------


SHOW TABLES

-- COMMAND ----------


SELECT * FROM cn_daily_customer_books

-- COMMAND ----------

SELECT * FROM fr_daily_customer_books

-- COMMAND ----------

SELECT * FROM author_counts_state

-- COMMAND ----------


