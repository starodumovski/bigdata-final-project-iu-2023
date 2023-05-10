INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT rev.critic_name, mov.genre, COUNT(*) AS reviews_amount
FROM reviews rev
INNER JOIN movies mov ON rev.rotten_tomatoes_link=mov.rotten_tomatoes_link
WHERE rev.critic_name IN (
	SELECT link_crit.critic_name
	FROM (
		SELECT DISTINCT rotten_tomatoes_link, critic_name
		FROM reviews
	) link_crit
	GROUP BY link_crit.critic_name
	HAVING count(*) > 900
)
GROUP BY rev.critic_name, mov.genre
ORDER BY rev.critic_name, mov.genre
;
