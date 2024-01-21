-- Databricks notebook source
CREATE OR REPLACE TABLE default.website_sessions AS 
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
  website_sessions

-- COMMAND ----------

SELECT * FROM website_sessions

-- COMMAND ----------

SELECT * FROM website_pageviews

-- COMMAND ----------


