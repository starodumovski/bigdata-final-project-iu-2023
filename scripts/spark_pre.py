#************** Import SparkSession *****************
from pyspark.sql import SparkSession 

spark = SparkSession \
        .builder \
        .appName("my spark app") \
        .master("local[3]") \
        .getOrCreate()
sc = spark.sparkContext


# ************* Cleaning Dataset ********************
import pyspark.sql.functions as F

df_movies = spark.read.format("csv") \
	.option("sep",",") \
	.option("header","true") \
	.option("inferSchema","true") \
	.load("./data/rotten_tomatoes_movies.csv")

df_reviews = spark.read.format("csv") \
	.option("sep",",") \
	.option("header","true") \
	.option("inferSchema","true") \
	.load("./data/spark_reviews.csv")
df_movies = df_movies.dropna().drop(F.col("movie_info")).drop(F.col("critics_consensus"))
df_reviews = df_reviews.dropna()
df_reviews = df_reviews.join(df_movies, on="rotten_tomatoes_link", how="inner") \
	.select(df_reviews.columns)
df_movies = df_movies.join(df_reviews, on="rotten_tomatoes_link", how="inner") \
	.select(df_movies.columns)
for column in df_movies.columns:
	df_movies = df_movies.withColumn(column, F.translate(F.col(column),"\"",""))
for column in df_reviews.columns:
	df_reviews = df_reviews.withColumn(column, F.translate(F.col(column),"\"",""))
df_reviews.coalesce(1).write \
	.mode("overwrite") \
	.format("com.databricks.spark.csv") \
	.option("header","true") \
	.csv("./data/clean_reviews")
df_movies.coalesce(1).write \
	.mode("overwrite") \
	.format("com.databricks.spark.csv") \
	.option("header","true") \
	.csv("./data/clean_movies") 
