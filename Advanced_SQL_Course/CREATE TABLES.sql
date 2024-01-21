-- Databricks notebook source
-- MAGIC %md
-- MAGIC # MAIN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## website_sessions

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.website_sessions AS 
SELECT
  website_session_id,
  created_at,
  user_id,
  is_repeat_session,
  CASE WHEN utm_source = 'NULL' THEN NULL ELSE utm_source END AS utm_source,
  CASE WHEN utm_campaign = 'NULL' THEN NULL ELSE utm_campaign END AS utm_campaign,
  CASE WHEN utm_content = 'NULL' THEN NULL ELSE utm_content END AS utm_content,
  device_type,
  CASE WHEN http_referer = 'NULL' THEN NULL ELSE http_referer END AS http_referer
FROM 
  divya_kunta_aws_8912329879554895.default.website_sessions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## website_pageviews

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.website_pageviews AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.website_pageviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## orders

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.orders AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## products

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.products AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## order_items

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.order_items AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## order_item_refunds

-- COMMAND ----------

CREATE OR REPLACE TABLE main.default.order_item_refunds AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.order_item_refunds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # HIVE_METASTORE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## website_sessions

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.website_sessions AS 
SELECT
  website_session_id,
  created_at,
  user_id,
  is_repeat_session,
  CASE WHEN utm_source = 'NULL' THEN NULL ELSE utm_source END AS utm_source,
  CASE WHEN utm_campaign = 'NULL' THEN NULL ELSE utm_campaign END AS utm_campaign,
  CASE WHEN utm_content = 'NULL' THEN NULL ELSE utm_content END AS utm_content,
  device_type,
  CASE WHEN http_referer = 'NULL' THEN NULL ELSE http_referer END AS http_referer
FROM 
  divya_kunta_aws_8912329879554895.default.website_sessions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## website_pageviews

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.website_pageviews AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.website_pageviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## orders

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.orders AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## products

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.products AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## order_items

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.order_items AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## order_item_refunds

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.default.order_item_refunds AS 
SELECT
  *
FROM 
  divya_kunta_aws_8912329879554895.default.order_item_refunds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # TESTING

-- COMMAND ----------

SELECT
website_pageview_id,
website_session_id
FROM website_pageviews

-- COMMAND ----------

SELECT
order_id,
user_id,
primary_product_id
FROM orders

-- COMMAND ----------

SELECT
  created_at,
  utm_source,
  utm_campaign,
  device_type
FROM  
  website_sessions

-- COMMAND ----------


