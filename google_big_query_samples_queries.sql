SELECT
      date,
      CURRENT_DATE() as the_date,
      EXTRACT(DAY FROM DATE '2013-12-25') as the_day_variant,
      EXTRACT(DAY FROM date) as the_day,
      EXTRACT(DAYOFWEEK FROM date) as day_of_week,
      EXTRACT(DAYOFYEAR FROM date) as day_of_year,
      EXTRACT(ISOYEAR FROM date) AS isoyear,
      EXTRACT(ISOWEEK FROM date) AS isoweek,
      EXTRACT(YEAR FROM date) AS year,
      EXTRACT(MONTH FROM date) AS month,
      EXTRACT(QUARTER FROM date) AS quarter,
      EXTRACT(WEEK FROM date) AS week,
      EXTRACT(WEEK(SUNDAY) FROM date) AS sunday_week,
      DATE(2019, 12, 04) as date_ymd,
      DATE(DATETIME "2019-12-04 23:59:59") as date_dt,
      DATE(TIMESTAMP "2019-12-04 05:30:00+07", "America/Los_Angeles") as date_tstz,
      DATE_ADD(date, INTERVAL 5 DAY) as five_days_later,
      DATE_ADD(date, INTERVAL 5 WEEK) as five_week_later,
      DATE_ADD(date, INTERVAL 5 MONTH) as five_months_later,
      DATE_ADD(date, INTERVAL 5 QUARTER) as five_quarters_later,
      DATE_ADD(date, INTERVAL 5 YEAR) as five_years_later,
      DATE_SUB(date, INTERVAL 5 DAY) as five_days_ago,
      DATE_SUB(date, INTERVAL 5 WEEK) as five_week_ago,
      DATE_SUB(date, INTERVAL 5 MONTH) as five_months_ago,
      DATE_SUB(date, INTERVAL 5 QUARTER) as five_quarters_ago,
      DATE_SUB(date, INTERVAL 5 YEAR) as five_years_ago,
      DATE_DIFF(CURRENT_DATE(), DATE '1983-11-17', DAY) as days_diff,
      DATE_DIFF(CURRENT_DATE(), DATE '1983-11-17', WEEK) as weeks_diff,
      DATE_DIFF(CURRENT_DATE(), DATE '1983-11-17', MONTH) as months_diff,
      DATE_DIFF(CURRENT_DATE(), DATE '1983-11-17', QUARTER) as quaters_diff,
      DATE_DIFF(CURRENT_DATE(), DATE '1983-11-17', YEAR) as years_diff,
      DATE_TRUNC(date, WEEK) as trunc_week,
      DATE_TRUNC(date, MONTH) as trunc_month,
      DATE_TRUNC(date, YEAR) as trunc_year,
      DATE_FROM_UNIX_DATE(18000) as date_from_epoch_in_days,
      FORMAT_DATE("%x", date) as US_format,
      PARSE_DATE("%x", "11/17/83") as parsed,
      UNIX_DATE(date) as days_from_epoch
FROM 
      UNNEST(GENERATE_DATE_ARRAY('2019-12-01', '2019-12-31')) AS date
ORDER BY 
      date;




SELECT 
  CURRENT_DATETIME() as now,
  DATETIME(2019, 12, 25, 05, 30, 00) as datetime_ymdhms,
  DATETIME('2019-12-25', "America/Los_Angeles") as dateexp_timeexp,
  DATETIME(TIMESTAMP "2019-12-25 05:30:00+00", "America/Los_Angeles") as datetime_tstz,
  DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 12 HOUR) as datetime_add_hour,
  DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 60 MINUTE) as datetime_add_minutes,
  DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 100 SECOND) as datetime_add_seconds,
  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 12 HOUR) as datetime_sub_hour,
  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 MINUTE) as datetime_sub_minutes,
  DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 100 SECOND) as datetime_sub_seconds,
  DATETIME_DIFF(CURRENT_DATETIME(), DATETIME "1983-11-17 00:00:01", DAY) as diff_days,
  DATETIME_DIFF(CURRENT_DATETIME(), DATETIME "1983-11-17 00:00:01", MICROSECOND) as diff_ms,
  DATETIME_TRUNC(CURRENT_DATETIME(), HOUR) as trunc_hour,
  DATETIME_TRUNC(CURRENT_DATETIME(), MINUTE) as trunc_minutes,
  FORMAT_DATETIME("%c", CURRENT_DATETIME()) AS formatted,
  PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1983-11-17 13:45:55') AS datetime_1,
  PARSE_DATETIME('%A, %B %e, %Y','Wednesday, December 19, 2018') AS datetime_2;
  
