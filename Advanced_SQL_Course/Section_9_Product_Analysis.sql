-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Practice

-- COMMAND ----------

SELECT 
  primary_product_id,
  COUNT(order_id) AS orders,
  round(SUM(price_usd),2) AS revenue,
  cast(round(sum(price_usd-cogs_usd),2) AS DOUBLE) AS margin,
  round(avg(price_usd),2) AS avg_price
FROM
  orders
WHERE
  order_id BETWEEN 10000 AND 11000
GROUP BY
  primary_product_id
ORDER BY
  COUNT(order_id) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Product Level Sales
-- MAGIC ## Monthly Trend of Sales,Revenue & Margin

-- COMMAND ----------

SELECT
  YEAR(created_at) AS year,
  month(created_at) AS month,
  count(DISTINCT order_id) AS number_of_sales,
  round(sum(price_usd),2) AS revenue,
  round(sum(price_usd-cogs_usd),2) AS margin
FROM
  orders
WHERE
  created_at < '2013-01-04'
GROUP BY
  YEAR(created_at),
  month(created_at)
ORDER BY
 YEAR(created_at),
 month(created_at) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Analyzing Product Lanches
-- MAGIC ## Monthly trend for order volume,overall conv rate,revenue per session & breakdown of sales by product

-- COMMAND ----------

SELECT
  year(website_sessions.created_at) AS year,
  month(website_sessions.created_at) AS month,
  count(DISTINCT orders.order_id) AS sales,
  round(count(DISTINCT orders.order_id)/count(DISTINCT website_sessions.website_session_id),2) AS conv_rate,
  round(sum(price_usd),2) AS revenue,
  count(DISTINCT website_sessions.website_session_id) AS sessions,
  round(sum(price_usd)/count(DISTINCT website_sessions.website_session_id),2) AS revenue_per_sessions,
  count(DISTINCT CASE WHEN primary_product_id = 1 THEN order_id ELSE NULL END) AS product_one_sales,
  count(DISTINCT CASE WHEN primary_product_id = 2 THEN order_id ELSE NULL END) AS product_two_sales 
FROM
  website_sessions
LEFT JOIN
  orders
  ON website_sessions.website_session_id = orders.website_session_id
WHERE
  website_sessions.created_at >'2012-04-01'
  AND website_sessions.created_at < '2013-04-05'
GROUP BY
  year(website_sessions.created_at),
  month(website_sessions.created_at)
ORDER BY
  year(website_sessions.created_at),
  month(website_sessions.created_at)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Product Level Website Analysis

-- COMMAND ----------

SELECT
  pageview_url,
  COUNT(DISTINCT website_pageviews.website_session_id) AS sessions,
  COUNT(DISTINCT orders.order_id) AS orders,
  round(COUNT(DISTINCT orders.order_id)/COUNT(DISTINCT website_pageviews.website_session_id),2) AS viewed_products_to_orders
FROM
  website_pageviews
LEFT JOIN
  orders
  ON website_pageviews.website_session_id = orders.website_session_id
WHERE 
  website_pageviews.created_at BETWEEN '2013-02-01' AND '2013-03-01'
  AND pageview_url IN ('/the-original-mr-fuzzy','/the-forever-love-bear')
GROUP BY
  pageview_url

-- COMMAND ----------


