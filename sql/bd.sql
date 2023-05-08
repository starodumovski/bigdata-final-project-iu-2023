--dropping the database
DROP DATABASE IF EXISTS project;
CREATE DATABASE project;

--switch the datababse
\c project;

-- dropping existing tables
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS reviews;

--create new tables
CREATE TABLE movies (
rotten_tomatoes_link VARCHAR(100) PRIMARY KEY NOT NULL,
movie_title VARCHAR(200),
content_rating VARCHAR(100),
genres VARCHAR(500),
directors VARCHAR(1000),
authors VARCHAR(1000),
actors VARCHAR(10000),
original_release_date timestamp,
streaming_release_date timestamp,
runtime numeric,
production_company VARCHAR(50),
tomatometer_status VARCHAR(20),
tomatometer_rating VARCHAR(20),
tomatometer_count numeric,
audience_status VARCHAR(10),
audience_rating numeric,
audience_count numeric,
tomatometer_top_critics_count numeric,
tomatometer_fresh_critics_count numeric,
tomatometer_rotten_critics_count numeric);

CREATE TABLE reviews (
rotten_tomatoes_link VARCHAR(100),
critic_name VARCHAR(100),
top_critic BOOLEAN,
publisher_name VARCHAR(100),
review_type VARCHAR(10),
review_score VARCHAR(20),
review_date timestamp,
review_content VARCHAR(500)
);

ALTER TABLE reviews
ADD CONSTRAINT fk_link
FOREIGN KEY (rotten_tomatoes_link)
REFERENCES movies (rotten_tomatoes_link);

--load the data in the tables 
\COPY movies(rotten_tomatoes_link,movie_title,content_rating,genres,directors,authors,actors,original_release_date,streaming_release_date,runtime,production_company,tomatometer_status,tomatometer_rating,tomatometer_count,audience_status,audience_rating,audience_count,tomatometer_top_critics_count,tomatometer_fresh_critics_count,tomatometer_rotten_critics_count)  FROM './data/clean_movies.csv'  DELIMITER ','  CSV HEADER;
\COPY reviews(rotten_tomatoes_link,critic_name,top_critic,publisher_name,review_type,review_score,review_date,review_content) FROM './data/clean_reviews.csv' DELIMITER ',' CSV HEADER;

select count(*) as movies_count from movies;
select count(*) as reviews_count from reviews;
