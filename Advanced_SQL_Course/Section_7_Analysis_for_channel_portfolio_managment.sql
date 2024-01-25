-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction

-- COMMAND ----------

SELECT 
	utm_content,
    COUNT(DISTINCT website_sessions.website_session_id) AS sessions,
    COUNT(DISTINCT orders.order_id) AS orders,
    COUNT(DISTINCT orders.order_id)/COUNT(DISTINCT website_sessions.website_session_id) AS sessions_conversation_rate
FROM
	website_sessions
LEFT JOIN
 	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at BETWEEN '2014-01-01' AND '2014-02-01'
 GROUP BY
 	utm_content
 ORDER BY
 	COUNT(DISTINCT website_sessions.website_session_id) desc

-- COMMAND ----------

SELECT 
    YEAR(website_sessions.created_at) AS year,
    MONTH(website_sessions.created_at) AS  month,
    COUNT(DISTINCT website_sessions.website_session_id) AS sessions,
    COUNT(DISTINCT orders.order_id) AS orders,
    COUNT(DISTINCT orders.order_id)/COUNT(DISTINCT website_sessions.website_session_id) AS conversation_rate
FROM
	website_sessions
LEFT JOIN 
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-11-27'
    AND website_sessions.utm_source = 'gsearch'
GROUP BY
	YEAR(website_sessions.created_at),
	MONTH(website_sessions.created_at);
    
    
SELECT
	  YEAR(website_sessions.created_at) AS year,
    MONTH(website_sessions.created_at) AS month,
	  COUNT(CASE WHEN utm_campaign = 'nonbrand' THEN website_sessions.website_session_id ELSE NULL END) AS nonbrand_sessions,
    COUNT(CASE WHEN utm_campaign = 'nonbrand' THEN orders.order_id ELSE NULL END) AS nonbrand_orders,
    COUNT(CASE WHEN utm_campaign = 'brand' THEN website_sessions.website_session_id ELSE NULL END) AS brand_sessions,
    COUNT(CASE WHEN utm_campaign = 'brand' THEN orders.order_id ELSE NULL END) AS brand_orders
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-11-27'
    AND website_sessions.utm_source = 'gsearch'
GROUP BY
	  YEAR(website_sessions.created_at),
    MONTH(website_sessions.created_at);
    
SELECT 
	  YEAR(website_sessions.created_at) AS year,
    MONTH(website_sessions.created_at) AS month,
    COUNT(CASE WHEN device_type = 'desktop' THEN website_sessions.website_session_id ELSE NULL END) AS desktop_sessions,
    COUNT(CASE WHEN device_type = 'desktop' THEN orders.order_id ELSE NULL END) AS desktop_orders,
    COUNT(CASE WHEN device_type = 'mobile' THEN website_sessions.website_session_id ELSE NULL END) AS mobile_sessions,
    COUNT(CASE WHEN device_type = 'mobile' THEN orders.order_id ELSE NULL END) AS mobile_orders
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-11-27'
    AND website_sessions.utm_source = 'gsearch'
    AND website_sessions.utm_campaign = 'nonbrand'
GROUP BY
	YEAR(website_sessions.created_at),
  MONTH(website_sessions.created_at) ;

SELECT 
  YEAR(website_sessions.created_at) AS year,
  month(website_sessions.created_at) AS month,
  COUNT(DISTINCT CASE WHEN utm_source = 'gsearch' THEN website_sessions.website_session_id ELSE NULL END) AS gsearch_sessions,
  COUNT(DISTINCT CASE WHEN utm_source = 'bsearch' THEN website_sessions.website_session_id ELSE NULL END) AS bsearch_sessions,
  COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN website_sessions.website_session_id ELSE NULL END) AS organic_sessions,
  COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN website_sessions.website_session_id ELSE NULL END) AS direct_type_in_sessions
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-11-27'
GROUP BY 
	YEAR(website_sessions.created_at),
    month(website_sessions.created_at);
    
SELECT
	  YEAR(website_sessions.created_at) AS year,
    MONTH(website_sessions.created_at) AS month,
    COUNT(website_sessions.website_session_id) AS sessions,
    COUNT(orders.order_id) AS orders,
	  COUNT(orders.order_id)/COUNT(website_sessions.website_session_id) AS conversation_rate
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at < '2012-11-27' 
GROUP BY
	  YEAR(website_sessions.created_at),
    MONTH(website_sessions.created_at);
-- HAVING	
--     website_sessions.created_at BETWEEN '2012-11-27' AND '2013-07-27'

-- SELECT * FROM orders
-- WHERE items_purchased > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analyzing Channel Portfolios

-- COMMAND ----------

SELECT
	-- WEEK(created_at) AS week,
	MIN(date(created_at)) AS week_start_date,
    -- COUNT(website_session_id) AS sessions,
    COUNT(CASE WHEN utm_source = 'gsearch' THEN website_session_id ELSE NULL END) AS gsearch_sessions,
    COUNT(CASE WHEN utm_source = 'bsearch' THEN website_session_id ELSE NULL END) AS bsearch_sessions
FROM 
	website_sessions
WHERE
	created_at > '2012-08-22'
	AND created_at < '2012-11-29'
    AND utm_campaign = 'nonbrand'
GROUP BY
	weekofyear(created_at);
    
    
SELECT
	utm_source,
	-- COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END) AS mobile_sessions,
    COUNT(DISTINCT CASE WHEN device_type = 'mobile' THEN website_session_id ELSE NULL END)/COUNT(DISTINCT website_session_id) AS pct_mobile
FROM
	website_sessions
WHERE
	created_at > '2012-08-22'
    AND created_at < '2012-11-30'
    AND utm_campaign = 'nonbrand'
GROUP BY
	utm_source;
    
    
    
SELECT
	* 
FROM
	website_sessions
WHERE
	created_at > '2012-08-22'
    AND created_at < '2012-11-30'
    -- AND utm_source = 'nonbrand'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Comparing Channels

-- COMMAND ----------

SELECT
	website_sessions.device_type,
    website_sessions.utm_source,
	COUNT(DISTINCT website_sessions.website_session_id) AS sessions,
    COUNT(DISTINCT orders.website_session_id) AS orders,
    COUNT(DISTINCT orders.website_session_id)/COUNT(DISTINCT website_sessions.website_session_id) AS conv_rate
FROM
	website_sessions
LEFT JOIN
	orders
    ON website_sessions.website_session_id = orders.website_session_id
WHERE
	website_sessions.created_at > '2012-08-22'
    AND website_sessions.created_at < '2012-09-18'
    AND website_sessions.utm_campaign = 'nonbrand'
GROUP BY
	website_sessions.device_type,
    website_sessions.utm_source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Cross-Channel Bid

-- COMMAND ----------

SELECT
	-- WEEK(created_at) AS week,
    MIN(DATE(created_at)) AS week_start_date,
    COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN utm_source = 'gsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) AS g_dtop_sessions,
    COUNT(DISTINCT CASE WHEN utm_source = 'bsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) AS b_dtop_sessions,
    COUNT(DISTINCT CASE WHEN utm_source = 'gsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) AS g_mob_sessions,
    COUNT(DISTINCT CASE WHEN utm_source = 'bsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) AS b_mob_sessions,
    COUNT(DISTINCT CASE WHEN utm_source = 'bsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END)/
    COUNT(DISTINCT CASE WHEN utm_source = 'gsearch' AND device_type = 'desktop' THEN website_session_id ELSE NULL END) AS b_pct_of_dtop,
    COUNT(DISTINCT CASE WHEN utm_source = 'bsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END)/
    COUNT(DISTINCT CASE WHEN utm_source = 'gsearch' AND device_type = 'mobile' THEN website_session_id ELSE NULL END) AS b_pct_of_mob
FROM
	website_sessions
WHERE
	created_at > '2012-11-04'
	AND created_at < '2012-12-22'
    AND utm_campaign = 'nonbrand'
    
GROUP BY
	weekofyear(created_at)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analyzing Channel Portfolio Trends

-- COMMAND ----------

SELECT
  CASE
    WHEN http_referer IS NULL THEN 'direct_type_in'
    WHEN http_referer = 'https://www.gsearch.com' AND utm_source IS NULL THEN 'gsearch_organic'
    WHEN http_referer = 'https://www.bsearch.com' AND utm_source IS NULL THEN 'bsearch_organic'
    ELSE 'other'
  END,
  COUNT(DISTINCT website_session_id) AS sessions
FROM
	website_sessions
WHERE
	website_session_id BETWEEN 100000 AND 115000
    -- AND utm_source IS NULL
GROUP BY
	 CASE
		WHEN http_referer IS NULL THEN 'direct_type_in'
        WHEN http_referer = 'https://www.gsearch.com' AND utm_source IS NULL THEN 'gsearch_organic'
        WHEN http_referer = 'https://www.bsearch.com' AND utm_source IS NULL THEN 'bsearch_organic'
        ELSE 'other'
	END
ORDER BY
	COUNT(DISTINCT website_session_id) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analyzing Direct Traffic
-- MAGIC

-- COMMAND ----------

SELECT
	YEAR(created_at) AS year,
    MONTH(created_at) AS month,
	COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id ELSE NULL END) AS nonbrand,
    COUNT(DISTINCT CASE WHEN utm_campaign = 'brand' THEN website_session_id ELSE NULL END) AS brand,
    COUNT(DISTINCT CASE WHEN utm_campaign = 'brand' THEN website_session_id ELSE NULL END)
    /COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id ELSE NULL END) AS brand_pct_of_nonbrand,
    COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IN('https://www.gsearch.com','https://www.bsearch.com') THEN website_session_id ELSE NULL END) AS organic_search,
    COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IN('https://www.gsearch.com','https://www.bsearch.com') THEN website_session_id ELSE NULL END)
    /COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id ELSE NULL END) AS organic_pct_of_nonbrand,
    COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN website_session_id ELSE NULL END) AS direct_type_in,
    COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN website_session_id ELSE NULL END)
    /COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' THEN website_session_id ELSE NULL END) AS direct_pct_of_nonbrand
FROM
	website_sessions
WHERE
	created_at < '2012-12-23'
GROUP BY
	YEAR(created_at),
	MONTH(created_at)

-- COMMAND ----------


