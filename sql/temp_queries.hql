USE projectdb;
INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT genre, COUNT(*) AS reviews_amount
FROM reviews
INNER JOIN movies ON reviews.rotten_tomatoes_link=movies.rotten_tomatoes_link
GROUP BY genre
ORDER BY reviews_amount DESC
;
INSERT OVERWRITE LOCAL DIRECTORY '/root/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT link_crit.critic_name, COUNT(*) as reviews_count
FROM (
	SELECT rotten_tomatoes_link, critic_name
	FROM reviews
) link_crit
GROUP BY link_crit.critic_name
ORDER BY reviews_count
;
INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT critic_name, genre, COUNT(*) AS reviews_amount
FROM reviews
INNER JOIN movies ON reviews.rotten_tomatoes_link=movies.rotten_tomatoes_link
WHERE critic_name IN (
	SELECT link_crit.critic_name
	FROM (
		SELECT rotten_tomatoes_link, critic_name
		SELECT reviews
	) link_crit
	GROUP BY link_crit.critic_name
	HAVING count(*) > 900
)
GROUP BY critic_name, genre
;
