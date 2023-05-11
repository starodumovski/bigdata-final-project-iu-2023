# ************** Import SparkSession *****************
"""Library that allows to work with spark dataframes and rrd"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

SPARK = SparkSession \
.builder \
.appName("my spark app") \
.master("local[3]") \
.getOrCreate()
SC = SPARK.sparkContext

# ************* WORK ON DATASETS ********************
DF_MOVIES = SPARK.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.option("quote", '"') \
.option("escape", '"') \
.load("./data/rotten_tomatoes_movies.csv")

DF_REVIEWS = SPARK.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.option("quote", '"') \
.option("escape", '"') \
.load("./data/spark_reviews.csv")

# *************** MOVIES EDITION ****************
COLUMNS_TO_DROP_M = ['movie_info', 'critics_consensus', 'actors']
COLUMNS_TO_CLEAN_M = ['movie_title', 'genres', 'directors', 'authors', 'production_company']
COLUMNS_TO_ARRAY_M = ['genres', 'directors', 'authors', 'production_company']

# DROP CASE *********
DF_MOVIES = DF_MOVIES.dropna()
for column in COLUMNS_TO_DROP_M:
    DF_MOVIES = DF_MOVIES.drop(F.col(column))
# CLEAN CASE ********
for column in COLUMNS_TO_CLEAN_M:
    DF_MOVIES = DF_MOVIES.withColumn(column, F.translate(F.col(column), '"', ''))
    DF_MOVIES = DF_MOVIES.withColumn(column, F.translate(F.col(column), "'", ''))
DF_MOVIES = DF_MOVIES.rdd.distinct().toDF()
# ARRAY CASE ********
for column in COLUMNS_TO_ARRAY_M:
    DF_MOVIES = DF_MOVIES.withColumn(column, F.concat(F.lit("{"), F.col(column), F.lit('}')))

# *************** REVIEWS EDITION ****************
COLUMNS_TO_CLEAN_R = ['critic_name', 'publisher_name', 'review_content']

# DROP CASE *********
DF_REVIEWS = DF_REVIEWS.dropna()
# CLEAN CASE ********
for column in COLUMNS_TO_CLEAN_R:
    DF_REVIEWS = DF_REVIEWS.withColumn(column, F.translate(F.col(column), '"', ''))
DF_REVIEWS = DF_REVIEWS.rdd.distinct().toDF()

# *************** RETRIVE INTERSECTION IN KEYS ****************
DF_REVIEWS = DF_REVIEWS.join(DF_MOVIES, on="rotten_tomatoes_link", how="inner") \
.select(DF_REVIEWS.columns)
DF_MOVIES = DF_MOVIES.join(DF_REVIEWS, on="rotten_tomatoes_link", how="inner") \
.select(DF_MOVIES.columns)

# ********************* WRITE IN FILES *************************
DF_REVIEWS.rdd.distinct().toDF().coalesce(1).write \
.mode("overwrite") \
.format("csv") \
.option("header", "true") \
.csv("./data/clean_reviews")
DF_MOVIES.rdd.distinct().toDF().coalesce(1).write \
.mode("overwrite") \
.format("csv") \
.option("header", "true") \
.csv("./data/clean_movies")
