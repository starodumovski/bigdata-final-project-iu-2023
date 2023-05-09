DROP DATABASE IF EXISTS projectdb CASCADE;
CREATE DATABASE projectdb;
USE projectdb;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

-- Create tables

-- movies table
CREATE EXTERNAL TABLE movies STORED AS AVRO LOCATION '/project/movies' TBLPROPERTIES ('avro.schema.url'='/project/avsc/movies.avsc');

-- reviews table
CREATE EXTERNAL TABLE reviews STORED AS AVRO LOCATION '/project/reviews' TBLPROPERTIES ('avro.schema.url'='/project/avsc/reviews.avsc');

-- reviews partitioned table
--CREATE EXTERNAL TABLE reviews_part(critic_name VARCHAR(100), top_critic BOOLEAN, publisher_name VARCHAR(100), review_type VARCHAR(10), review_score VARCHAR(20), review_date timestamp, review_content VARCHAR(500)) PARTITIONED BY (rotten_tomatoes_link VARCHAR(100)) STORED AS AVRO LOCATION '/project/reviews_part' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

-- INSERT INTO reviews_part partition (rotten_tomatoes_link) SELECT * FROM reviews;

-- Check
SELECT count(*) as movies_count FROM movies;
SELECT count(*) as reviews_count FROM reviews;
