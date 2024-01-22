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


