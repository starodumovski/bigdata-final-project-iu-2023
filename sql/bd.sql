--dropping the database
DROP DATABASE IF EXISTS project;
CREATE DATABASE project;

--switch the datababse
\c project;

--create new tables
CREATE TABLE movies (
rotten_tomatoes_link VARCHAR(100),
movie_title VARCHAR(200),
content_rating VARCHAR(100),
genres VARCHAR(100)[],
directors VARCHAR(100)[],
authors VARCHAR(100)[],
original_release_date timestamp,
streaming_release_date timestamp,
runtime numeric,
production_company VARCHAR(100),
tomatometer_status VARCHAR(20),
tomatometer_rating VARCHAR(20),
tomatometer_count numeric,
audience_status VARCHAR(10),
audience_rating numeric,
audience_count numeric,
tomatometer_top_critics_count numeric,
tomatometer_fresh_critics_count numeric,
tomatometer_rotten_critics_count numeric);


\COPY movies(rotten_tomatoes_link,movie_title,content_rating,genres,directors,authors,original_release_date,streaming_release_date,runtime,production_company,tomatometer_status,tomatometer_rating,tomatometer_count,audience_status,audience_rating,audience_count,tomatometer_top_critics_count,tomatometer_fresh_critics_count,tomatometer_rotten_critics_count)  FROM './data/clean_movies.csv'  DELIMITER ','  CSV HEADER;

CREATE TABLE movies_1 AS (SELECT *, unnest(genres) as genre FROM movies);
DROP TABLE movies;
CREATE TABLE movies_2 AS (SELECT *, unnest(authors) as author FROM movies_1);
DROP TABLE movies_1;
CREATE TABLE movies AS (SELECT *, unnest(directors) as director FROM movies_2);
DROP TABLE movies_2;

CREATE TABLE links AS (SELECT DISTINCT rotten_tomatoes_link FROM movies);
ALTER TABLE links ADD CONSTRAINT pk_link PRIMARY KEY (rotten_tomatoes_link);
ALTER TABLE movies ADD CONSTRAINT fk_link_movies FOREIGN KEY (rotten_tomatoes_link) REFERENCES links (rotten_tomatoes_link);
ALTER TABLE movies DROP COLUMN genres, DROP COLUMN authors, DROP COLUMN directors;

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


ALTER TABLE reviews ADD CONSTRAINT fk_link_reviews FOREIGN KEY (rotten_tomatoes_link) REFERENCES links (rotten_tomatoes_link);

--load the data in the tables 
\COPY reviews(rotten_tomatoes_link,critic_name,top_critic,publisher_name,review_type,review_score,review_date,review_content) FROM './data/clean_reviews.csv' DELIMITER ',' CSV HEADER;

select count(*) as links_count from links;
select count(*) as movies_count from movies;
select count(*) as reviews_count from reviews;


