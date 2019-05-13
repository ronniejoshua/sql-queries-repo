/* Basics: Count, Min, and Max */

SELECT * 
FROM store_sales;

SELECT * 
FROM store_sales 
LIMIT 10;

SELECT count(*) 
FROM store_sales;

SELECT Month_of_year, count(*) 
FROM store_sales 
GROUP BY month_of_year;

SELECT MAX(employee_shifts) 
FROM store_sales;

SELECT MIN(employee_shifts) 
FROM store_sales;

SELECT MIN(employee_shifts), MAX(employee_shifts) 
FROM store_sales;

SELECT month_of_year, MIN(employee_shifts), MAX(employee_shifts)
FROM store_sales
GROUP BY month_of_year;


/* Sum and Average */

SELECT *
FROM store_sales
LIMIT 10;

SELECT SUM(units_sold) 
FROM store_sales;

SELECT month_of_year, SUM(employee_shifts)
FROM store_sales
GROUP BY month_of_year;

SELECT month_of_year, SUM(units_sold), avg(units_sold)
FROM store_sales
GROUP BY month_of_year;


/* Variance and Standard Deviation */

SELECT 
		month_of_year, 
		sum(units_sold), 
		avg(units_sold), 
		var_pop(units_sold)
FROM store_sales
GROUP BY month_of_year;


SELECT
  month_of_year, 
  SUM(units_sold), 
  AVG(units_sold), 
  VAR_POP(units_sold), 
  STDDEV_POP(units_sold)
FROM store_sales
GROUP BY Month_of_year;


select
  month_of_year,
  SUM(units_sold)AS total_units_sold,
  ROUND(AVG(units_sold),2) AS avg_units_sold, 
  ROUND(VAR_POP(units_sold), 2) AS var_units_sold,
  ROUND(STDDEV_POP(units_sold), 2) AS stdev_units_sold
FROM store_sales
GROUP BY Month_of_year;


/* Discrete Percentiles & Continuous Percentiles */

SELECT * 
FROM store_sales 
LIMIT 5;

SELECT * 
FROM store_sales 
ORDER BY revenue DESC;

SELECT ROUND(AVG(revenue),2) AS avg_revenue 
FROM store_sales;

SELECT
    PERCENTILE_DISC(0.50) WITHIN GROUP(ORDER BY revenue) as pct_50_rev 
FROM store_sales;


SELECT
   PERCENTILE_DISC(0.50) WITHIN GROUP(ORDER BY revenue) as pct_50_rev,
   PERCENTILE_DISC(0.60) WITHIN GROUP(ORDER BY revenue) as pct_60_rev,
   PERCENTILE_DISC(0.90) WITHIN GROUP(ORDER BY revenue) as pct_90_rev,
   PERCENTILE_DISC(0.95) WITHIN GROUP(ORDER BY revenue) as pct_95_rev
FROM store_sales;

SELECT
    PERCENTILE_CONT(0.95) WITHIN GROUP (order by revenue) as pct_95c_rev,
    PERCENTILE_DISC(0.95) WITHIN GROUP (order by revenue) as pct_95d_rev 
FROM store_sales;


/* Correlation Coefficients */


SELECT
   ROUND(CORR(units_sold, revenue)::numeric, 2) AS CORR_usold_revenue,
   ROUND(CORR(units_sold, employee_shifts)::numeric, 2) AS CORR_usold_eshifts,
   ROUND(CORR(units_sold, month_of_year)::numeric, 2) AS CORR_usold_moy
FROM store_sales;

/* mode */

SELECT
   month_of_year,
   MODE() WITHIN GROUP (ORDER BY employee_shifts) as emp_mode
FROM store_sales
GROUP BY month_of_year;


/* Row_Number */

SELECT
   sale_date,
   month_of_year,
   units_sold,
   ROW_NUMBER () OVER (ORDER BY units_sold)
FROM store_sales;


SELECT
   sale_date,
   month_of_year,
   units_sold,
   ROW_NUMBER () OVER (ORDER BY units_sold)
FROM store_sales
ORDER BY sale_date;



/* Computing Intercept & Slope */

SELECT
    ROUND(REGR_INTERCEPT(employee_shifts, units_sold)::numeric,2) as regr_intercept,
	ROUND(REGR_SLOPE(employee_shifts, units_sold)::numeric, 4) as regr_slope
FROM store_sales;



SELECT
    ROUND(REGR_INTERCEPT(units_sold, employee_shifts)::numeric,2) as regr_intercept,
	ROUND(REGR_SLOPE(units_sold, employee_shifts)::numeric,2) as regr_slope
FROM store_sales;


SELECT
	ROUND(
			(REGR_SLOPE(employee_shifts, units_sold) * 1500 + REGR_INTERCEPT(employee_shifts, units_sold))::numeric, 2
		 ) as predicted_value
FROM store_sales;