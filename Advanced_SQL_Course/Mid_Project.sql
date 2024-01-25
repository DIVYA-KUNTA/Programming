-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Mid Project

-- COMMAND ----------

SELECT
	MIN(website_pageview_id) AS first_pageview_id
FROM
	website_pageviews
GROUP BY
	pageview_url = '/billing-2';

WITH T1 AS (
SELECT
	website_pageviews.website_session_id,
    website_pageviews.pageview_url AS billing_version_seen,
    orders.order_id
FROM
	website_pageviews
LEFT JOIN
	orders
    ON website_pageviews.website_session_id = orders.website_session_id
WHERE
	website_pageviews.website_pageview_id >= 53550
    AND website_pageviews.created_at < '2012-11-10'
    AND website_pageviews.pageview_url IN('/billing','/billing-2'))
    
SELECT 
	billing_version_seen,
    COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT order_id)/COUNT(DISTINCT website_session_id) AS billing_to_order_rate
FROM
	T1
GROUP BY
	billing_version_seen

-- COMMAND ----------


