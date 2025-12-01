-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Schema Evolution

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE;
CREATE DATABASE IF NOT EXISTS demodb;
USE demodb;


-- COMMAND ----------

SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Schema Validations Summary
-- MAGIC - `INSERT INTO`
-- MAGIC   - Column matching by position, New columns not allowed
-- MAGIC - `INSERT OVERWRITE`
-- MAGIC   - Column matching by position, New columns not allowed
-- MAGIC - `MERGE .. INSERT`
-- MAGIC   - Column matching by name, New columns ignored
-- MAGIC - `DataFrame Append`
-- MAGIC   - Column matching by name, New columns not allowed
-- MAGIC - `Data Type Mismatch`
-- MAGIC   - Not allowed in any case
-- MAGIC
-- MAGIC #####Schema evolution approaches
-- MAGIC - `Manual` - New columns
-- MAGIC - `Automatic` - New columns

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Manual Schema Evolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Manual schema evolution - New column at the end

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

ALTER TABLE people ADD COLUMNS (birthDate STRING);

-- COMMAND ----------

DESC people

-- COMMAND ----------

-- DBTITLE 1,Columns by position
INSERT INTO people
SELECT id, fname, lname, dob
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

DESC HISTORY people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

-- DBTITLE 1,Columns by name
INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate, current_date() toDay
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

INSERT OVERWRITE people
SELECT id, fname firstName, lname lastName
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Manual schema evolution - New column in the middle

-- COMMAND ----------

ALTER TABLE people ADD COLUMNS (phoneNumber STRING after lastName);

-- COMMAND ----------

DESC people

-- COMMAND ----------

SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate, phone phoneNumber
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Automatic Schema Evolution - Session Level

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

SELECT * FROM people;

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Automatic schema evolution - New column at the end

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json` 

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Automatic schema evolution - New column in the middle
-- MAGIC For INSERT 
-- MAGIC 1. Either it doesn't work because of the column matching by position
-- MAGIC 2. Or it corrupts your data

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle
-- MAGIC Works with MERGE INSERT

-- COMMAND ----------

MERGE INTO people T
USING 
(
    SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate 
    FROM json.`dbfs:/FileStore/data/schema/people_3.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN 
    INSERT *

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Automatic Schema Evolution at Table level

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

SELECT * FROM people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Schema evolution - New column at the end

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_2_schema = "id INT, fname STRING, lname STRING, dob STRING"
-- MAGIC
-- MAGIC people_2_df =  (
-- MAGIC       spark
-- MAGIC       .read
-- MAGIC       .format("json")
-- MAGIC       .schema(people_2_schema)
-- MAGIC       .load("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC       .toDF("id", "firstName", "lastName", "birthDate")
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC      people_2_df
-- MAGIC       .write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("people")
-- MAGIC )

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_3_schema = "id INT, fname STRING, lname STRING, phone STRING, dob STRING"
-- MAGIC
-- MAGIC people_3_df =  (
-- MAGIC       spark
-- MAGIC       .read
-- MAGIC       .format("json")
-- MAGIC       .schema(people_3_schema)
-- MAGIC       .load("dbfs:/FileStore/data/schema/people_3.json")
-- MAGIC       .toDF("id", "firstName", "lastName", "phoneNumber", "birthDate")
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC    people_3_df
-- MAGIC       .write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("people")
-- MAGIC )

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Cleanup

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------


