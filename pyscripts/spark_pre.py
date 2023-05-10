# ************** Import SparkSession *****************
from pyspark.sql import SparkSession 

spark = SparkSession \
        .builder \
        .appName("my spark app") \
        .master("local[3]") \
        .getOrCreate()
sc = spark.sparkContext


# ************* WORK ON DATASETS ********************
import pyspark.sql.functions as F

df_movies = spark.read.format("csv") \
	.option("header","true") \
	.option("inferSchema","true") \
	.option("quote", '"') \
	.option("escape", '"') \
	.load("./data/rotten_tomatoes_movies.csv")

df_reviews = spark.read.format("csv") \
	.option("header","true") \
	.option("inferSchema","true") \
	.option("quote", '"') \
	.option("escape", '"') \
	.load("./data/spark_reviews.csv")

# *************** MOVIES EDITION ****************
columns_to_drop_m = ['movie_info', 'critics_consensus', 'actors']
columns_to_clean_m = ['movie_title', 'genres', 'directors', 'authors', 'production_company']
columns_to_array_m = ['genres', 'directors', 'authors', 'production_company']

# DROP CASE *********
df_movies = df_movies.dropna()
for column in columns_to_drop_m:
	df_movies = df_movies.drop(F.col(column))
# CLEAN CASE ********
for column in columns_to_clean_m:
	df_movies = df_movies.withColumn(column, F.translate(F.col(column), '"', ''))
	df_movies = df_movies.withColumn(column, F.translate(F.col(column), "'", ''))
df_movies = df_movies.rdd.distinct().toDF()
# ARRAY CASE ********
for column in columns_to_array_m:
	df_movies = df_movies.withColumn(column, F.concat(F.lit("{"), F.col(column), F.lit('}')))

# *************** REVIEWS EDITION ****************
columns_to_clean_r = ['critic_name', 'publisher_name', 'review_content']

# DROP CASE *********
df_reviews = df_reviews.dropna()
# CLEAN CASE ********
for column in columns_to_clean_r:
	df_reviews = df_reviews.withColumn(column, F.translate(F.col(column), '"', ''))
df_reviews = df_reviews.rdd.distinct().toDF()

# *************** RETRIVE INTERSECTION IN KEYS ****************
df_reviews = df_reviews.join(df_movies, on="rotten_tomatoes_link", how="inner") \
	.select(df_reviews.columns)
df_movies = df_movies.join(df_reviews, on="rotten_tomatoes_link", how="inner") \
	.select(df_movies.columns)

# ********************* WRITE IN FILES *************************
df_reviews.rdd.distinct().toDF().coalesce(1).write \
	.mode("overwrite") \
	.format("csv") \
	.option("header","true") \
	.csv("./data/clean_reviews")
df_movies.rdd.distinct().toDF().coalesce(1).write \
	.mode("overwrite") \
	.format("csv") \
	.option("header","true") \
	.csv("./data/clean_movies") 
