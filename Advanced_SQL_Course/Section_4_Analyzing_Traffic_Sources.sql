-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analyzing Top Traffic Sources

-- COMMAND ----------

SELECT
    utm_source,
    utm_campaign,
    http_referer,
    COUNT(DISTINCT website_session_id) AS sessions    
FROM
	main.default.website_sessions
 WHERE
	created_at  < '2012-04-12'
GROUP BY
	utm_source,
    utm_campaign,
    http_referer
ORDER BY
	sessions DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Traffic Source Conversion Rates

-- COMMAND ----------

SELECT
	COUNT(DISTINCT website_sessions.website_session_id) AS sessions,
    COUNT(DISTINCT order_id) AS orders,
   COUNT(DISTINCT order_id)/ COUNT(DISTINCT website_sessions.website_session_id) AS session_to_order_conv_rate
FROM
	website_sessions
LEFT JOIN orders
	ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-04-14'
	AND utm_source = 'gsearch'
    AND utm_campaign = 'nonbrand'

-- COMMAND ----------


