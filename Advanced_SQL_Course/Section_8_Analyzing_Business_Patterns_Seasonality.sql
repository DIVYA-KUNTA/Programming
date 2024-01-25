-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Practice Question

-- COMMAND ----------

SELECT
	website_session_id,
	created_at,
    HOUR(created_at) AS hour,
    WEEKDAY(created_at) AS weekday,
    CASE 
		WHEN WEEKDAY(created_at) = 0 THEN 'MONDAY'
		WHEN WEEKDAY(created_at) = 1 THEN 'TUESDAY'
        WHEN WEEKDAY(created_at) = 2 THEN 'WEDNESDAY'
        WHEN WEEKDAY(created_at) = 3 THEN 'THURSDAY'
		ELSE 'OTHER_DAY'
	END AS clean_day,
    quarter(created_at) AS qua,
    MONTH(created_at) AS month,
    DATE(created_at) AS date,
    weekofyear(created_at) AS week
FROM
	website_sessions
WHERE
	website_session_id BETWEEN 150000 AND 155000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analyzing Seasonality

-- COMMAND ----------

SELECT
	YEAR(website_sessions.created_at) AS year,
	-- MONTH(website_sessions.created_at) AS month,
    MIN(DATE(website_sessions.created_at)) AS week_start_date,
    weekofyear(website_sessions.created_at) AS week,
    COUNT(website_sessions.website_session_id) AS sessions,
    COUNT(orders.order_id) AS orders
    -- COUNT(orders.order_id)/COUNT(website_sessions.website_session_id) AS ord_pct_of_sessions
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2013-01-01'
GROUP BY
	YEAR(website_sessions.created_at),
    -- MONTH(website_sessions.created_at)
    weekofyear(website_sessions.created_at)
    -- MIN(DATE(website_sessions.created_at)

-- COMMAND ----------


