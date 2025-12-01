-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Time Travel 
-- MAGIC **Time Travel feature allows us to rollback a specific version of the table**

-- COMMAND ----------


DESCRIBE HISTORY users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

UPDATE users SET age = 100 WHERE id = 1

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

-- SELECT * FROM users VERSION AS OF 1
-- SELECT * FROM users@v1
SELECT * FROM users

-- COMMAND ----------

RESTORE TABLE users VERSION AS OF 7

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

RESTORE TABLE users TIMESTAMP AS OF '2025-11-20 08:55:00'

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------


