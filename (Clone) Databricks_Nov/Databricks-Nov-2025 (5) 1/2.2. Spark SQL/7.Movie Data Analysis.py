# Databricks notebook source
# MAGIC %md
# MAGIC **Dataset: dbfs:/FileStore/ctsdatasets/movielens**
# MAGIC
# MAGIC
# MAGIC - From movies.csv and ratings.csv datasets, fetch the top 10 movies with highest average user-rating
# MAGIC 	- Consider only those movies that are rated by atleast 50 users
# MAGIC 	- Data: movieId, title, totalRatings, averageRating
# MAGIC 	- Arrange the data in the DESC order of averageRating
# MAGIC 	- Round the averageRating to 4 decimal places
# MAGIC 	- Save the output as a single pipe-separated CSV file with header
# MAGIC 	- Use only DF transformation methods (not SQL)

# COMMAND ----------

movies_file = "dbfs:/FileStore/ctsdatasets/movielens/movies.csv"
ratings_file = "dbfs:/FileStore/ctsdatasets/movielens/ratings.csv"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


