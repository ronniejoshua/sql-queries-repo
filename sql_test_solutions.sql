
-- My Solution
-- write your code in PostgreSQL 9.4

WITH will_borad_bus AS
  (SELECT p.id AS pid,
          min(cast(b.time AS TIME)) AS dtime
   FROM passengers AS p
   LEFT JOIN buses AS b ON p.origin = b.origin
   AND p.destination = b.destination
   AND cast(p.time AS TIME) <= cast(b.time AS TIME)
   GROUP BY 1)



SELECT id,
       pob
FROM
  (SELECT id,
          cast(TIME AS TIME),
          count(wbb.dtime) as pob
   FROM buses AS b
   LEFT JOIN will_borad_bus AS wbb ON cast(b.time AS TIME) = wbb.dtime
   GROUP BY 1,
            2) AS RESULT
ORDER BY id ASC;




-- SOF Solution
-- write your code in PostgreSQL 9.4
WITH will_board_bus AS (
    SELECT
        p.id AS pid,
        MIN(CAST(b.time AS TIME)) AS departure_time
    FROM
        passengers AS p
    LEFT JOIN buses AS b ON (b.origin = p.origin
            AND b.destination = p.destination
            AND CAST(b.time AS TIME) >= CAST(p.time AS TIME))
GROUP BY
    p.id
)
SELECT
    b.id,
    COUNT(departure_time)
FROM
    will_board_bus AS wbb
    LEFT JOIN passengers AS p ON wbb.pid = p.id
    LEFT JOIN buses AS b ON (p.origin = b.origin
            AND p.destination = b.destination)
        AND (wbb.departure_time = CAST(b.time AS TIME)
            OR departure_time IS NULL)
WHERE
    b.id IS NOT NULL
GROUP BY
    b.id
ORDER BY
    b.id;
   
   
   
-- Question Test Status & Test Case
-- write your code in PostgreSQL 9.4
SELECT
    tg.name AS name,
    count(tc.status) AS all_test_cases,
    sum(CASE WHEN tc.status LIKE 'OK' THEN 1 ELSE 0 END) AS passed_test_cases,
    tg.test_value * sum( CASE WHEN tc.status LIKE 'OK' THEN 1 ELSE 0 END) AS total_value
FROM
    test_groups AS tg
    LEFT JOIN test_cases AS tc ON tg.name = tc.group_name
GROUP BY
    tg.name,
    tg.test_value
ORDER BY
    total_value DESC,
    name;
