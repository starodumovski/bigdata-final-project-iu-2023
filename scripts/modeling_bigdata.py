import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


spark = SparkSession.builder\
    .appName("BDT Project")\
    .master("local[*]")\
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
    .config("spark.sql.catalogImplementation","hive")\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .config("spark.jars", 
            "file:///usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,file:///usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar")\
    .config("spark.jars.packages","org.apache.spark:spark-avro_2.12:3.0.3")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
sqlContext = SQLContext(sc)




from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

q1_schema = StructType([ \
    StructField("rotten_tomatoes_link",StringType(),True), \
    StructField("top_critic",BooleanType(),True), \
    StructField("publisher_name",StringType(),True), \
    StructField("review_type", StringType(), True), \
    StructField("review_score", StringType(), True), \
    StructField("review_date", IntegerType(), True), \
    StructField("review_content", IntegerType(), True) \
  ])


movies_schema = StructType([ \
    StructField("rotten_tomatoes_link",StringType(),True), \
    StructField("movie_title",StringType(),True), \
    StructField("content_rating",StringType(),True), \
    StructField("genres", StringType(), True), \
    StructField("directors", StringType(), True), \
    StructField("authors", IntegerType(), True), \
    StructField("actors", IntegerType(), True), \
    StructField("original_release_date",StringType(),True), \
    StructField("streaming_release_date",BooleanType(),True), \
    StructField("runtime",StringType(),True), \
    StructField("production_company", StringType(), True), \
    StructField("tomatometer_status", StringType(), True), \
    StructField("tomatometer_rating", IntegerType(), True), \
    StructField("tomatometer_count", IntegerType(), True), \
    StructField("audience_status",BooleanType(),True), \
    StructField("audience_rating",StringType(),True), \
    StructField("audience_count", StringType(), True), \
    StructField("tomatometer_top_critics_count", StringType(), True), \
    StructField("tomatometer_fresh_critics_count", IntegerType(), True), \
    StructField("tomatometer_rotten_critics_count", IntegerType(), True) \
  ])



movies = sqlContext.read.format('csv').options(header='true', schema=movies_schema, inferschema='true').load('full_movies.csv')
q1 = sqlContext.read.format('csv').options(header='true', schema=q1_schema, inferschema='true').load('reviews_part_00.csv')
q2 = sqlContext.read.format('csv').options(header='false', inferschema='true').load('reviews_part_01.csv')
q3 = sqlContext.read.format('csv').options(header='false', inferschema='true').load('reviews_part_02.csv')
q4 = sqlContext.read.format('csv').options(header='false', inferschema='true').load('reviews_part_03.csv')
q5 = sqlContext.read.format('csv').options(header='false', inferschema='true').load('reviews_part_04.csv')
q6 = sqlContext.read.format('csv').options(header='false', inferschema='true').load('reviews_part_05.csv')





reviews = q1.join(movies, q1.rotten_tomatoes_link ==  movies.rotten_tomatoes_link,"inner")
reviews = reviews.drop(reviews.runtime)
reviews = reviews.drop(reviews.review_content)
reviews = reviews.drop(reviews.movie_title)
reviews = reviews.drop(reviews.publisher_name)
reviews = reviews.drop(reviews.review_type)


reviews = reviews.drop(reviews.review_date)


def score_imputer(val):
    if val[0] == 'A':
        return 95.0
    elif val[0] == 'B':
        return 80.0
    elif val[0] == 'C':
        return 65.0
    elif '/' in val:
        x = val.split('/', 1)
        return 100*float(int(x[0])/int(x[1]))
    else:
        return val






def score_imputer(val):
    if val[0] == 'A':
        return 95.0
    elif val[0] == 'B':
        return 80.0
    elif val[0] == 'C':
        return 65.0
    elif '/' in val:
        x = val.split('/', 1)
        return 100*float(int(x[0])/int(x[1]))
    elif '.' in val:
        x = val.split('.', 1)
        return float(x[0]) + float(x[1])/10.0
    else:
        return val

udf_func = F.udf(score_imputer, DoubleType())


reviews = reviews.withColumn('review_score', udf_func(F.col('review_score')))
reviews = reviews.withColumn('review_score', reviews['review_score'].cast('double'))
reviews = reviews.withColumn('original_release_date', reviews['original_release_date'].cast('int'))
reviews = reviews.withColumn('streaming_release_date', reviews['streaming_release_date'].cast('int'))
reviews = reviews.withColumn('tomatometer_rating', reviews['tomatometer_rating'].cast('double'))
# reviews = reviews.withColumn('streaming_release_date', from_unixtime(reviews['streaming_release_date']))
# reviews = reviews.withColumn('original_release_date', from_unixtime(reviews['original_release_date']))

reviews = reviews.withColumn('tomatometer_count', reviews['tomatometer_count'].cast('double'))
reviews = reviews.withColumn('audience_count', reviews['audience_count'].cast('double'))
reviews = reviews.withColumn('audience_rating', reviews['audience_rating'].cast('double'))
reviews = reviews.withColumn('tomatometer_top_critics_count', reviews['tomatometer_top_critics_count'].cast('double'))
reviews = reviews.withColumn('tomatometer_fresh_critics_count', reviews['tomatometer_fresh_critics_count'].cast('double'))
reviews = reviews.withColumn('tomatometer_rotten_critics_count', reviews['tomatometer_rotten_critics_count'].cast('double'))
reviews = reviews.withColumn('content_rating', reviews['content_rating'].cast('double'))
reviews = reviews.withColumn('top_critic', reviews['top_critic'].cast('string'))



from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol='critic_name', outputCol='critic_name_numeric').fit(reviews)
df = indexer.transform(reviews)
df = df.drop(df.critic_name)

indexer = StringIndexer(inputCol='top_critic', outputCol='top_critic_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.top_critic)

indexer = StringIndexer(inputCol='production_company', outputCol='production_company_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.production_company)

indexer = StringIndexer(inputCol='tomatometer_status', outputCol='tomatometer_status_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.tomatometer_status)

indexer = StringIndexer(inputCol='audience_status', outputCol='audience_status_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.audience_status)

indexer = StringIndexer(inputCol='genre', outputCol='genre_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.genre)

indexer = StringIndexer(inputCol='author', outputCol='author_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.author)

indexer = StringIndexer(inputCol='director', outputCol='director_numeric').fit(df)
df = indexer.transform(df)
df = df.drop(df.director)

#This set will be later used to showcase the model functionality
to_be_predicted = df.filter(df.review_score.isNull())
df = df.na.drop(subset=['review_score'])


# There was a duplicate column (with duplicate name) in the data, the goal of this cell was to erase it
df_cols = df.columns


duplicate_col_index = [idx for idx,
  val in enumerate(df_cols) if val in df_cols[:idx]]
  

for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicate'
  

df = df.toDF(*df_cols)

df = df.drop(df.rotten_tomatoes_link)
df = df.drop(df.rotten_tomatoes_link_duplicate)



print(type(df.columns))


from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['content_rating', 'original_release_date', 
                                               'streaming_release_date', 'tomatometer_rating', 'tomatometer_count', 
                                               'audience_rating', 'audience_count', 'tomatometer_top_critics_count'
                                               , 'tomatometer_fresh_critics_count', 
                                               'tomatometer_rotten_critics_count',
                                               'critic_name_numeric', 'top_critic_numeric', 
                                               'production_company_numeric', 'tomatometer_status_numeric', 
                                               'audience_status_numeric', 'genre_numeric', 'author_numeric', 
                                               'director_numeric'], outputCol = 'features')

vdf = vectorAssembler.transform(df)

vdf = vdf.select(['features', 'review_score'])

splits = vdf.randomSplit([0.75, 0.25])

train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='review_score', maxIter=10, regParam=0.3, elasticNetParam=0.8)

lr_model = lr.fit(train_df)



print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'review_score')

dt_model = dt.fit(train_df)
print("here")
dt_predictions = dt_model.transform(test_df)
dt_evaluator = RegressionEvaluator(
    labelCol="review_score", predictionCol="prediction", metricName="rmse")
rmse = dt_evaluator.evaluate(dt_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)



paramGrid = ParamGridBuilder() \
    .addGrid(HashingTF.numFeatures, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

crossval = CrossValidator(estimator=lr_model,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(),
                          numFolds=3)


cvModel = crossval.fit(train_df)


paramGrid = ParamGridBuilder() \
    .addGrid(HashingTF.numFeatures, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

crossval = CrossValidator(estimator=dt,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(),
                          numFolds=3)


cvModel = crossval.fit(train_df)


predictions = dt_model.transform(to_be_predicted).show()


