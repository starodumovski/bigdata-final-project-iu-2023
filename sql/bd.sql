-- dropping existing tables
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS reviews;

--create new tables
CREATE TABLE movies (
rotten_tomatoes_link VARCHAR(100) PRIMARY KEY NOT NULL,
movie_title VARCHAR(200),
content_rating VARCHAR(10),
genres text[],
directors text[],
authors text[],
actors text[],
original_release_date timestamp,
streaming_release_date timestamp,
runtime numeric,
production_company text,
tomatometer_status text,
tomatometer_rating text,
tomatometer_count numeric,
audience_status text,
audience_rating numeric,
audience_count numeric,
tomatometer_top_critics_count numeric,
tomatometer_fresh_critics_count numeric,
tomatometer_rotten_critics_count numeric);

--load the data in the tables 
\COPY movies(rotten_tomatoes_link,movie_title,content_rating,genres,directors,authors,actors,original_release_date,streaming_release_date,runtime,production_company,tomatometer_status,tomatometer_rating,tomatometer_count,audience_status,audience_rating,audience_count,tomatometer_top_critics_count,tomatometer_fresh_critics_count,tomatometer_rotten_critics_count)  FROM 'project_git/data/movies.csv'  DELIMITER ','  CSV HEADER;
