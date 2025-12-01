-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Creating Delta Lake Table

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

DROP TABLE IF EXISTS users;

-- COMMAND ----------

CREATE TABLE users (
  id INT,
  name STRING,
  age INT,
  gender STRING
)
TBLPROPERTIES (
  delta.enableDeletionVectors = true
)

-- COMMAND ----------

CREATE TABLE users (
  id INT,
  name STRING,
  age INT,
  gender STRING
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Insert rows in the table

-- COMMAND ----------

INSERT INTO users VALUES (1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),(3, "Ramya", 38, "Female");
INSERT INTO users VALUES (4, "Ravi", 28, "Male"), (5, "Raheem", 25, "Male"), (6, "Radha", 45, "Female");
INSERT INTO users VALUES (7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male"), (9, "Rahul", 25, "Male");
INSERT INTO users VALUES (10, "Rohit", 34, "Male"),(11, "Ramana", 32, "Male"), (12, "Rehman", 55, "Male");

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC EXTENDED users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

UPDATE users SET age = age + 1 WHERE gender = 'Female'

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000005.json

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000006.json

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

SELECT * FROM users VERSION AS OF 4

-- COMMAND ----------

DELETE FROM users WHERE id IN (2, 4)

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC EXTENDED users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000007.json

-- COMMAND ----------


