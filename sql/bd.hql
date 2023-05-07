DROP DATABASE IF EXISTS projectdb CASCADE;
CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

-- Create tables

-- emps table
CREATE EXTERNAL TABLE movies STORED AS AVRO LOCATION '/project/movies' TBLPROPERTIES ('avro.schema.url'='/project/avsc/movies.avsc');

-- Check

SELECT * FROM movies LIMIT 1;

