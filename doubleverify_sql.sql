/*
fact_impressions - table definition            
+---------------+----------+
| user_id       | int      |
| site          | varchar  |
| ts            | timestamp|    
+---------------+----------+
*Each row is an impression:
 an impression is created when a user enters a site and is exposed to an ad
*Size: Billions of daily records
*Retention: Saved for 3 days

analysts_rules.xlsx - excel file           
+------------+---------------+------------+-----------------+
| site       | num of events | time frame | action          |
+------------+---------------+------------+-----------------+
| ynet.co.il | 1000          | 2 hours    | mark as fraud   |
+------------+---------------+------------+-----------------+
| ynet.co.il | 200           | 5 days     | unmark as fraud |
+------------+---------------+------------+-----------------+
| cnn.com    | 15000         | 14 hours   | mark as fraud   |
+------------+---------------+------------+-----------------+
| cnn.com    | 75000         | 1 week     | unmark as fraud |
+------------+---------------+------------+-----------------+
*The rules define under which circumstances a user should be marked and unmarked as fraud
*More roles will be added by the analysts over time
*Size: currently a few hundreds, might grow up to a few thousands of records

Requirement:
Plan an hourly process that will produce a list of user ids and whether they should be marked or unmarked as fraud
(this list will be later on be taken by the server developers and these users will be blocked)
*/
DROP TABLE IF EXISTS FACT_IMPRESSIONS;

CREATE TABLE FACT_IMPRESSIONS (
    IMPRESSION_ID INT AUTO_INCREMENT, 
    USER_ID INT, SITE VARCHAR(255), 
    TS TIMESTAMP, 
    PRIMARY KEY(IMPRESSION_ID)
);


INSERT INTO fact_impressions (user_id, site, ts)
VALUES (2, 'site.com', '2020-03-01 01:30:00.000000'),
      (3, 'site.com', '2020-03-01 01:45:00.000000'),
      (1, 'site.com', '2020-03-01 02:00:00.000000'),
      (3, 'site.com', '2020-03-01 02:00:00.000000'),
      (1, 'site.com', '2020-03-01 02:05:00.000000'),
      (3, 'site.com', '2020-03-01 02:15:00.000000'),
      (1, 'site.com', '2020-03-01 02:20:00.000000'),
      (2, 'site.com', '2020-03-01 02:30:00.000000'),
      (1, 'site.com', '2020-03-01 02:35:00.000000'),
      (1, 'site.com', '2020-03-01 02:50:00.000000'),
      (1, 'site.com', '2020-03-01 02:55:00.000000');
   
SELECT * FROM fact_impressions;


DROP TABLE IF EXISTS ANALYSTS_RULES;

CREATE TABLE analysts_rules (
    site VARCHAR(255), 
    action_needed VARCHAR(255), 
    num_of_events INT, 
    time_frame_hours INT, 
    PRIMARY KEY(
        site, action_needed
    )
);

INSERT
    INTO analysts_rules (
    site, 
    action_needed, 
    num_of_events, 
    time_frame_hours)
VALUES ('site.com', 'fraud', 5, 1),
       ('site.com', 'unfraud', 3, 2);
   
SELECT * FROM analysts_rules;

-- Solution:
-- Create an Intermediate table, and push 3 days aggregated data into it
-- agg table
DROP TABLE IF EXISTS fact_imps_agg;
CREATE TABLE fact_imps_agg AS
SELECT
    user_id,
    site,
    DATE_FORMAT(ts, '%Y-%m-%d %H:00:00') AS date_hour,
    count(*) cnt_imps
FROM
    fact_impressions
GROUP BY
    1, 2, 3
ORDER BY
    1, 2, 3;

SELECT
    *
FROM
    fact_imps_agg;



-- Solution 1
EXPLAIN 
SELECT
  agg.user_id,
  rules.action_needed
FROM
  fact_imps_agg agg
  JOIN analysts_rules rules ON agg.site = rules.site
WHERE
  agg.date_hour >= TIMESTAMPADD(
    HOUR,
    rules.time_frame_hours * -1,
    '2020-03-01 03:00:00'
  ) --CURRENT_TIMESTAMP())
GROUP BY
  1,
  2
HAVING
  CASE
    WHEN action_needed = 'fraud' THEN SUM(cnt_imps) >= MAX(num_of_events)
    WHEN action_needed = 'unfraud' THEN SUM(cnt_imps) < MAX(num_of_events)
  END;

-- Solution 2
EXPLAIN
SELECT
    *
FROM
    (
        SELECT
            *, sum(cnt_imps) OVER(
                PARTITION BY user_id, site
            ORDER BY
                date_hour ASC ROWS UNBOUNDED PRECEDING
            ) AS cum_events, TIMESTAMPDIFF(
                HOUR, min(date_hour) OVER(
                    PARTITION BY user_id, site ROWS UNBOUNDED PRECEDING
                ), max(date_hour) OVER(
                    PARTITION BY user_id, site ROWS UNBOUNDED PRECEDING
                )
            )+ 1 AS hours_since
        FROM
            fact_imps_agg
        ORDER BY
            user_id, site, date_hour ASC
    ) AS ci
LEFT JOIN analysts_rules AS ar ON
    ci.site = ar.site
    AND ci.cum_events >= ar.num_of_events
    AND hours_since <= ar.time_frame_hours
WHERE
    action_needed LIKE 'fraud';
