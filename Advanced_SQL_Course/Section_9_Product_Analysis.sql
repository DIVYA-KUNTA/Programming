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

-- MAGIC %md
-- MAGIC # Assignment_1
-- MAGIC ## product level website

-- COMMAND ----------

SELECT
  *
FROM
  website_pageviews
WHERE 
  pageview_url = '/products'


-- COMMAND ----------

SELECT
  *
FROM
  website_pageviews
WHERE
  website_session_id = 6

-- COMMAND ----------

WITH T1 AS 
(
  SELECT
    website_session_id,
    website_pageview_id AS product_pageview_id
  FROM
    website_pageviews
  WHERE
    pageview_url = '/products'
  ORDER BY
    website_session_id ASC
),

T2 AS
(
  SELECT
    website_session_id,
    max(website_pageview_id) AS max_pageview_id
  FROM
    website_pageviews
  WHERE
    created_at BETWEEN '2013-01-06' AND '2013-04-06'
  GROUP BY
    website_session_id
),

T3 AS
(
  SELECT
    T2.website_session_id,
    orders.primary_product_id,
    T2.max_pageview_id,
    T1.product_pageview_id,
    CASE WHEN max_pageview_id > product_pageview_id THEN 'yes' ELSE NULL END AS clickthrough
  FROM T2
    LEFT JOIN T1
      ON T2.website_session_id = T1.website_session_id  
    LEFT JOIN orders
    ON T2.website_session_id = orders.website_session_id  
  WHERE
    product_pageview_id IS NOT NULL
  ORDER BY
    T2.website_session_id
)

SELECT
  primary_product_id,
  count(DISTINCT website_session_id) AS sessions,
  COUNT(CASE WHEN clickthrough = 'yes' THEN 1 END) AS clickedthrough_product,
  round(COUNT(CASE WHEN clickthrough = 'yes' THEN 1 END)/count(DISTINCT website_session_id),2) AS clickthrough_rate 
FROM T3
GROUP BY
  primary_product_id


-- COMMAND ----------

SELECT
  *
FROM
  orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Product Level Website Analysis-Solution 

-- COMMAND ----------

WITH  product_pageviews AS 
(
SELECT
  website_session_id,
  website_pageview_id,
  created_at,
  CASE
      WHEN created_at <'2013-01-01' THEN 'A. pre_product_2'
      WHEN created_at >= '2013-01-01' THEN 'B. post_product_2'
      ELSE 'uh oh.....check logic'
  END AS time_period
FROM website_pageviews
WHERE
  created_at < '2013-01-01'
  AND created_at > '2012-10-06'
  AND pageview_url = '/products'
),

sessions_w_next_pageview_id AS 
(
SELECT
  product_pageviews.time_period,
  product_pageviews.website_session_id,
  MIN(website_pageviews.website_pageview_id) AS min_next_pageview_id
FROM product_pageviews
LEFT JOIN website_pageviews
  ON product_pageviews.website_session_id = website_pageviews.website_session_id
  AND website_pageviews.website_pageview_id > product_pageviews.website_pageview_id
GROUP BY
  product_pageviews.time_period,
  product_pageviews.website_session_id
  
),

sesions_w_next_pageview_url AS
(
SELECT
  sessions_w_next_pageview_id.time_period,
  sessions_w_next_pageview_id.website_session_id,
  website_pageviews.pageview_url
FROM sessions_w_next_pageview_id
LEFT JOIN website_pageviews
  ON sessions_w_next_pageview_id.min_next_pageview_id = website_pageviews.website_pageview_id 
)


-- SELECT
--   time_period,
--   count(DISTINCT website_session_id) AS sessions,
--   count(DISTINCT CASE WHEN pageview_url IS NOT NULL THEN website_session_id ELSE NULL END) AS w_next_page,
--   count(DISTINCT CASE WHEN pageview_url IS NOT NULL THEN website_session_id ELSE NULL END)/ Count(DISTINCT website_session_id) AS pct_w_next_page,
--   count(DISTINCT CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN website_session_id ELSE NULL END) AS to_mrfuzzy,
--   count(DISTINCT CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN website_session_id ELSE NULL END)/count(DISTINCT website_session_id) AS pct_to_mr_fuzzy,
--   count(DISTINCT CASE WHEN pageview_url = '/the-forever-love-bear' THEN website_session_id ELSE NULL END) AS to_lovebear,
--   count(DISTINCT CASE WHEN pageview_url = '/the-forever-love-bear' THEN website_session_id ELSE NULL END)/count(DISTINCT website_session_id) AS pct_to_lovebear
-- FROM sesions_w_next_pageview_url
-- GROUP BY
--   time_period

SELECT * FROM sesions_w_next_pageview_url


-- COMMAND ----------


