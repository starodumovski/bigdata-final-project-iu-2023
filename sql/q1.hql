INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT genre, COUNT(*) AS reviews_amount
FROM reviews
INNER JOIN movies ON reviews.rotten_tomatoes_link=movies.rotten_tomatoes_link
GROUP BY genre
ORDER BY reviews_amount DESC
;
