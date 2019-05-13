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



















