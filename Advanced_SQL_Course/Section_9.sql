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


