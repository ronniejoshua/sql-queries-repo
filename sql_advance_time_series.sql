/* Create a Time Series Schema */
CREATE SCHEMA time_series AUTHORIZATION postgres;

/* Create a table of location and temperature measurements */
CREATE TABLE time_series.location_temp (
    event_time timestamp without time zone NOT NULL,
    temp_celcius integer,
    location_id character varying
);

ALTER TABLE time_series.location_temp OWNER TO postgres;

/* Create table of server monitoring metrics */
CREATE TABLE time_series.utilization (
    event_time timestamp without time zone NOT NULL,
    server_id integer NOT NULL,
    cpu_utilization real,
    free_memory real,
    session_cnt integer,
    CONSTRAINT utilization_pkey PRIMARY KEY (event_time, server_id)
);

ALTER TABLE time_series.utilization OWNER TO postgres;

/* Loading Data */
COPY time_series.location_temp (event_time,
    location_id,
    temp_celcius)
FROM
    '/Users/ronniejoshua/Downloads/location_temp.txt' DELIMITER ',';

/* baselin execution time without index */
EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
GROUP BY
    location_id;

EXPLAIN ANALYZE
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
GROUP BY
    location_id;

CREATE INDEX idx_loc_temp_location_id ON time_series.location_temp (location_id);

/* execution location with index */
EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
GROUP BY
    location_id;

/* add where clause, this is when index is helpful */
EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
WHERE
    location_id = 'loc2'
GROUP BY
    location_id;

/* Without index, WHERE clause takes much longer */
DROP INDEX time_series.idx_loc_temp_location_id;

EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
WHERE
    location_id = 'loc2'
GROUP BY
    location_id;

/* Create a table of location and temperature measurements */
CREATE TABLE time_series.location_temp_p (
    event_time timestamp NOT NULL,
    event_hour integer,
    temp_celcius integer,
    location_id character varying
)
PARTITION BY RANGE (event_hour);

CREATE TABLE time_series.loc_temp_p1 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (0) TO (2);

CREATE INDEX idx_loc_temp_p1 ON time_series.loc_temp_p1 (event_time);

CREATE TABLE time_series.loc_temp_p2 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (2) TO (4);

CREATE INDEX idx_loc_temp_p2 ON time_series.loc_temp_p2 (event_time);

CREATE TABLE time_series.loc_temp_p3 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (4) TO (6);

CREATE INDEX idx_loc_temp_p3 ON time_series.loc_temp_p3 (event_time);

CREATE TABLE time_series.loc_temp_p4 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (6) TO (8);

CREATE INDEX idx_loc_temp_p4 ON time_series.loc_temp_p4 (event_time);

CREATE TABLE time_series.loc_temp_p5 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (8) TO (10);

CREATE INDEX idx_loc_temp_p5 ON time_series.loc_temp_p5 (event_time);

CREATE TABLE time_series.loc_temp_p6 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (10) TO (12);

CREATE INDEX idx_loc_temp_p6 ON time_series.loc_temp_p6 (event_time);

CREATE TABLE time_series.loc_temp_p7 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (12) TO (14);

CREATE INDEX idx_loc_temp_p7 ON time_series.loc_temp_p7 (event_time);

CREATE TABLE time_series.loc_temp_p8 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (14) TO (16);

CREATE INDEX idx_loc_temp_p8 ON time_series.loc_temp_p8 (event_time);

CREATE TABLE time_series.loc_temp_p9 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (16) TO (18);

CREATE INDEX idx_loc_temp_9 ON time_series.loc_temp_p9 (event_time);

CREATE TABLE time_series.loc_temp_p10 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (18) TO (20);

CREATE INDEX idx_loc_temp_p10 ON time_series.loc_temp_p10 (event_time);

CREATE TABLE time_series.loc_temp_p11 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (20) TO (22);

CREATE INDEX idx_loc_temp_p11 ON time_series.loc_temp_p11 (event_time);

CREATE TABLE time_series.loc_temp_p12 PARTITION OF time_series.location_temp_p
FOR VALUES FROM (22) TO (24);

CREATE INDEX idx_loc_temp_p12 ON time_series.loc_temp_p12 (event_time);

INSERT INTO time_series.location_temp_p (event_time, event_hour, temp_celcius, location_id) (
    SELECT
        event_time,
        extract(hour FROM event_time),
        temp_celcius,
        location_id
    FROM
        time_series.location_temp);

EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06'
GROUP BY
    location_id;

EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp_p
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06'
GROUP BY
    location_id;

EXPLAIN
SELECT
    location_id,
    avg(temp_celcius)
FROM
    time_series.location_temp_p
WHERE
    event_hour BETWEEN 0 AND 4
GROUP BY
    location_id;

/* Loading Data */
COPY time_series.utilization (event_time,
    server_id,
    cpu_utilization,
    free_memory,
    session_cnt)
FROM
    '/Users/ronniejoshua/Downloads/utilization.txt' DELIMITER ',';

EXPLAIN
SELECT
    server_id,
    avg(cpu_utilization)
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06'
GROUP BY
    server_id;

CREATE INDEX idx_util_time_serv ON time_series.utilization (event_time, server_id);

EXPLAIN
SELECT
    server_id,
    avg(cpu_utilization)
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06'
GROUP BY
    server_id;

DROP INDEX time_series.idx_util_time_serv;

CREATE INDEX dx_util_time_serv ON time_series.location_temp (server_id, event_time);

EXPLAIN
SELECT
    server_id,
    avg(cpu_utilization)
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06'
GROUP BY
    server_id;

DROP INDEX time_series.idx_util_time_serv;

/* Using Views in PosgreSQL */
CREATE VIEW time_series.v_utilization AS (
    SELECT
        *,
        server_id % 10 AS dept_id
    FROM
        time_series.utilization
);

SELECT
    dept_id,
    server_id,
    cpu_utilization,
    lead(cpu_utilization) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC) AS lead_tplus1,
    lag(cpu_utilization) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC) AS lag_tminus1,
    lead(cpu_utilization, 10) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC) AS lead_tplus10,
    lag(cpu_utilization, 10) OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC) AS lag_tminus10,
    rank() OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC),
    ROUND(percent_rank() OVER (PARTITION BY dept_id ORDER BY cpu_utilization DESC)::numeric, 6)
FROM
    time_series.v_utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06';

/* Common Table Expression */
-----------------------------

WITH daily_avg_temp AS (
    SELECT
        date_trunc('day', event_time) AS event_date,
        avg(temp_celcius) AS avg_temp
    FROM
        time_series.location_temp
    GROUP BY
        date_trunc('day', event_time)
)
SELECT
    event_date,
    avg_temp
FROM
    daily_avg_temp;

/* Aggregation Over Windows */
-----------------------------

SELECT
    server_id,
    cpu_utilization,
    avg(cpu_utilization) OVER (PARTITION BY server_id)
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06';

/* Aggregation Over Windows - Comparision */
--------------------------------------------

WITH daily_avg_temp AS (
    SELECT
        date_trunc('day', event_time) AS event_date,
        avg(temp_celcius) AS avg_temp
    FROM
        time_series.location_temp
    GROUP BY
        date_trunc('day', event_time)
)
SELECT
    event_date,
    avg_temp,
    (
        SELECT
            avg_temp
        FROM
            daily_avg_temp AS dat2
        WHERE
            dat2.event_date = dat1.event_date - interval '1' day) AS previous_avg_temp
FROM
    daily_avg_temp AS dat1;

/* MOVING AVERAGE */
--------------------

SELECT
    event_time,
    server_id,
    avg(cpu_utilization) OVER (ORDER BY event_time ROWS BETWEEN 12 PRECEDING AND CURRENT ROW) AS hourly_cpu_util
FROM
    time_series.utilization;

/* WEIGHTED MOVING AVERAGE */
-----------------------------

WITH daily_avg_temp AS (
    SELECT
        date_trunc('day', event_time) AS event_date,
        avg(temp_celcius) AS avg_temp
    FROM
        time_series.location_temp
    GROUP BY
        date_trunc('day', event_time)
)
SELECT
    event_date,
    round(avg_temp, 2),
    (
        SELECT
            round(avg_temp * 0.5, 2)
        FROM
            daily_avg_temp AS dat2
        WHERE
            date_trunc('day', dat1.event_date) - interval '1' day = date_trunc('day', dat2.event_date)) + (
        SELECT
            round(avg_temp * 0.333, 2)
        FROM
            daily_avg_temp AS dat2
        WHERE
            date_trunc('day', dat1.event_date) - interval '2' day = date_trunc('day', dat2.event_date)) + (
        SELECT
            round(avg_temp * 0.167, 2)
        FROM
            daily_avg_temp AS dat2
        WHERE
            date_trunc('day', dat1.event_date) - interval '2' day = date_trunc('day', dat2.event_date)) AS wmp_temp
FROM
    daily_avg_temp AS dat1;

/* Forecasting Liner Regression */
----------------------------------

/* y = mx + b
 m = slope
 b = intercept
 x = input value
 y = predicted value
 */
SELECT
    regr_slope(free_memory, cpu_utilization) AS slope,
    regr_intercept(free_memory, cpu_utilization) AS intercept
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06';

SELECT
    regr_slope(free_memory, cpu_utilization) * 0.60 + regr_intercept(free_memory, cpu_utilization) AS PV_free_memory
FROM
    time_series.utilization
WHERE
    event_time BETWEEN '2019-03-05'
    AND '2019-03-06';

