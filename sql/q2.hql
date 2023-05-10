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
