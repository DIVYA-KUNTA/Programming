-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ALL_QUERIES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_1

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

