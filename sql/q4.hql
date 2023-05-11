INSERT OVERWRITE LOCAL DIRECTORY '/root/q4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT q1.critic_name, q1.genre, (q1.fresh_amount / (q1.fresh_amount + q2.rotten_amount)) * 100 AS liked_percent  
FROM (
	SELECT rev.critic_name, mov.genre, COUNT(*) AS fresh_amount
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
	) AND rev.review_type = 'Fresh'
	GROUP BY rev.critic_name, mov.genre, rev.review_type
) q1
INNER JOIN (
	SELECT rev.critic_name, mov.genre, COUNT(*) AS rotten_amount
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
	) AND rev.review_type = 'Rotten'
	GROUP BY rev.critic_name, mov.genre, rev.review_type
) q2
ON q1.critic_name=q2.critic_name AND q1.genre=q2.genre
ORDER BY q1.critic_name, q1.genre
