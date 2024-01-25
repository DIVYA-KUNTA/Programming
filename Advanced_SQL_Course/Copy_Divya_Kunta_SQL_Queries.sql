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


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_2

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

-- MAGIC %md
-- MAGIC ## Query_3

-- COMMAND ----------


WITH T1 AS (
SELECT
	website_session_id,
    MIN(website_pageview_id) AS min_pageview_id
FROM
	website_pageviews
WHERE
	created_at < '2012-06-14'
GROUP BY 
	website_session_id),

T2 AS (
SELECT 
	T1.website_session_id,
    website_pageviews.pageview_url As landing_page
FROM
	T1
LEFT JOIN
	website_pageviews
	ON T1.min_pageview_id = website_pageviews.website_pageview_id
WHERE
	website_pageviews.pageview_url = '/home'),
    
 T3 AS (
SELECT 
	T2.website_session_id,
    T2.landing_page,
    COUNT(website_pageviews.website_pageview_id) AS count_of_pages_viewed
FROM
	T2
LEFT JOIN
	website_pageviews
    ON T2.website_session_id = website_pageviews.website_session_id
GROUP BY
	T2.website_session_id,
    T2.landing_page
HAVING
	COUNT(website_pageviews.website_pageview_id) = 1)
	
SELECT 
	COUNT(DISTINCT T2.website_session_id) AS sessions,
    COUNT(DISTINCT T3.website_session_id) AS bounced_sessions,
    COUNT(DISTINCT T3.website_session_id)/COUNT(DISTINCT T2.website_session_id) AS bonuce_rate
FROM
	T2
lEFT JOIN
	T3
    ON T2.website_session_id = T3.website_session_id
    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_4

-- COMMAND ----------

SELECT
	MIN(created_at) AS first_created_at,
    MIN(website_pageview_id) AS first_pageview_id
FROM
	website_pageviews
WHERE 
	pageview_url = '/lander-1'
    AND created_at  IS NOT NULL;
WITH T1 AS (    
SELECT
	website_pageviews.website_session_id,
   MIN( website_pageviews.website_pageview_id) AS min_pageview_id
FROM
	website_pageviews
LEFT JOIN
	website_sessions
    ON website_pageviews.website_session_id = website_sessions.website_session_id
    AND website_sessions.created_at < '2012-07-28'
    AND website_pageviews.website_pageview_id > 23504
    AND utm_source = 'gsearch'
    AND utm_campaign = 'non brand'
GROUP By
	website_pageviews.website_session_id),

T2 AS (
SELECT 
	T1.website_session_id,
    website_pageviews.website_pageview_id AS min_pageview_id
FROM
	T1
LEFT JOIN
	website_pageviews
    ON T1.min_pageview_id = website_pageviews.website_pageview_id
WHERE
	website_pageviews.pageview_url IN('/home','/lander-1')),
T3 AS (    
SELECT 
	T2.website_session_id,
    website_pageviews.pageview_url As landing_page
FROM
	T2
LEFT JOIN
	website_pageviews
    ON T2.min_pageview_id = website_pageviews.website_pageview_id
WHERE
	website_pageviews.pageview_url In('/home','/lander-1')),
 T4 AS (   
SELECT 
	T3.website_session_id,
    T3.landing_page,
   COUNT(website_pageviews.website_pageview_id) AS count_of_pages_viewed
FROM
	T3
LEFT JOIN
	website_pageviews
    ON T3.website_session_id = website_pageviews.website_session_id
GROUP BY
	T3.website_session_id,
    T3.landing_page
HAVING
	COUNT(website_pageviews.website_pageview_id) = 1)
		
        
SELECT 
	T3.landing_page,
	COUNT(DISTINCT T3.website_session_id) AS sessions,
    COUNT(DISTINCT T4.website_session_id) AS count_bounced_website_session_id,
    COUNT(DISTINCT T4.website_session_id)/COUNT(DISTINCT T3.website_session_id) AS bounce_rate
FROM
	T3
LEFT JOIN
	T4
	ON T3.website_session_id = T4.website_session_id
GROUP BY
	T3.landing_page


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_5

-- COMMAND ----------


WITH T1 AS (
SELECT 
	website_session_id,
	MIN(website_pageview_id) AS min_pageview_id
FROM
	website_pageviews
WHERE 
	created_at >= '2012-06-01' AND created_at <= '2012-08-31'
GROUP BY
	website_session_id),

T2 AS (
SELECT 
	T1.website_session_id,
    T1.min_pageview_id,
	website_pageviews.pageview_url
FROM
	T1
LEFT JOIN
	website_pageviews
    ON T1.min_pageview_id = website_pageviews.website_pageview_id
WHERE
	pageview_url in  ('/home','/lander-1')),
    
T3 As (
SELECT 
	T2.website_session_id,
    T2.min_pageview_id,
    T2.pageview_url,
    website_sessions.utm_campaign
FROM
	T2
LEFT JOIN
	website_sessions
    ON T2.website_session_id = website_sessions.website_session_id
WHERE
	utm_campaign = 'nonbrand'),
    
T4 AS
(SELECT
	weekofyear(created_at) AS WEEK,
    MIN(created_at) AS week_start
FROM
	T3
LEFT JOIN
	website_sessions
    ON T3.website_session_id = website_sessions.website_session_id
GROUP BY
	weekofyear(created_at)),

T5 AS(
SELECT 
	weekofyear(created_at) AS week,
    T3.pageview_url AS Landing_Page,
    COUNT(T3.website_session_id) AS count_of_sessions
FROM
	T3
LEFT JOIN
	website_sessions
    ON T3.website_session_id = website_sessions.website_session_id
GROUP BY
	weekofyear(created_at),
    T3.pageview_url),
    
T6 AS (SELECT 
	T4.week_start,
    T5.landing_page,
    T5.count_of_sessions
FROM
	T4
LEFT JOIN
	T5
    ON T4.week = T5.week)
    
select  week_start,
  sum(case when landing_page = '/home' then count_of_sessions else 0 end) home,
  sum(case when landing_page = '/lander-1' then count_of_sessions else 0 end) lander
from T6
group by week_start

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_6

-- COMMAND ----------

SELECT 
  * 
FROM products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_7

-- COMMAND ----------

WITH T1 AS(
SELECT
	website_session_id,
    created_at,
    weekofyear(created_at) AS week    
FROM
	website_pageviews),
T2 AS (
SELECT 
	week,
    min(created_at) AS week_start
    FROM
	T1
GROUP BY
	week),
T3 AS (    
SELECT
	T1.website_session_id,
    T2.week_start
FROM
	T1
LEFT JOIN
	T2
    ON T1.week = T2.week),
 T4 AS (   
SELECT
	T3.week_start,
    COUNT(distinct T3.website_session_id) AS week_total_sessions
FROM
	T3
GROUP BY
	T3.week_start),
T5_BOU AS (    
SELECT
	website_session_id,
    COUNT(distinct website_pageview_id) AS count_of_pageviews
FROM
	website_pageviews
GROUP BY
	website_session_id
HAVING
	COUNT(distinct website_pageview_id) = 1),
T6 AS (
SELECT
	T5_BOU.website_session_id,
	T3.week_start
FROM
	T5_BOU
LEFT JOIN
	T3
    ON T5_BOU.website_session_id = T3.website_session_id),
T7 AS (
SELECT
	T6.week_start,
    COUNT(T6.website_session_id) AS bounced_sessions
FROM
	T6
GROUP BY
	T6.week_start)
    
SELECT
	T4.week_start,
    T4.week_total_sessions,
    T7.bounced_sessions,
    T7.bounced_sessions/T4.week_total_sessions AS bounce_rate
FROM
	T4
LEFT JOIN
	T7
    ON T4.week_start = T7.week_start
    

	


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_8

-- COMMAND ----------


WITH T1 AS (
SELECT
	website_sessions.website_session_id,
    website_sessions.created_at,
    MIN(website_pageviews.website_pageview_id) AS first_pageview_id,
	COUNT(website_pageviews.website_pageview_id) AS count_pageviews
FROM
	website_sessions
LEFT JOIN
	website_pageviews
    ON website_sessions.website_session_id = website_pageviews.website_pageview_id
WHERE
	website_sessions.created_at > '2012-06-01'
    AND website_sessions.created_at < '2012-08-31'
    AND website_sessions.utm_source ='gsearch'
    AND website_sessions.utm_campaign = 'nonbrand'
GROUP BY
	website_sessions.website_session_id),
    
T2 AS (
SELECT 
	T1.website_session_id,
    T1.created_at,
    T1.first_pageview_id,
    T1.count_pageviews,
    website_pageviews.pageview_url AS landing_page,
    website_pageviews.created_at AS sessions_created_at
FROM
	T1
LEFT JOIN
	website_pageviews
    ON T1.first_pageview_id = website_pageviews.website_pageview_id),
    
T3 AS (    
SELECT 
	concat(year(sessions_created_at),weekofyear(sessions_created_at)) AS year_week,
  MIN(DATE(sessions_created_at)) AS week_start_date,
  COUNT(DISTINCT website_session_id) As total_sessions,
  COUNT(DISTINCT CASE WHEN count_pageviews = 1 THEN website_session_id ELSE NULL END) AS bounce_sessions,
  COUNT(DISTINCT CASE WHEN count_pageviews = 1 THEN website_session_id ELSE NULL END)/COUNT(DISTINCT website_session_id) AS bounce_rate,
  COUNT(DISTINCT CASE WHEN landing_page = '/home' THEN website_session_id ELSE NULL END) AS home_sessions,
  COUNT(DISTINCT CASE WHEN landing_page = '/lander-1' THEN website_session_id ELSE NULL END) AS lander_sessions
FROM
	T2
GROUP BY
  year(sessions_created_at),weekofyear(sessions_created_at)
	)
    
SELECT * FROM T3
    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_9

-- COMMAND ----------

SELECT 
	website_sessions.website_session_id,
    website_pageviews.pageview_url,
    website_pageviews.created_at AS pageview_created_at,
    CASE WHEN pageview_url = '/product' THEN 1 ELSE 0 END AS product_page,
    CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS mrfuzzy_page,
    CASE WHEN pageview_url = '/cart' THEN 1 ELSE 0 END AS cart_page
FROM
	website_sessions
LEFT JOIN
	website_pageviews
    ON website_sessions.website_session_id = website_pageviews.website_session_id
WHERE
	website_sessions.created_at BETWEEN '2014-01-01' AND '2014-02-01'
    AND website_pageviews.pageview_url IN('/lander-2','/product','/the-original-mr-fuzzy','/cart')
ORDER BY
website_sessions.website_session_id,
website_pageviews.created_at;

WITH session_level_made_it_flags_demo AS (
SELECT
	website_session_id,
    MAX(product_page) AS product_made_it,
    MAX(mrfuzzy_page) AS mrfuzzy_made_it,
    MAX(cart_page) AS cart_made_it
    
FROM
		(
		SELECT 
			website_sessions.website_session_id,
			website_pageviews.pageview_url,
			website_pageviews.created_at AS pageview_created_at,
			CASE WHEN pageview_url = '/products' THEN 1 ELSE 0 END AS product_page,
			CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS mrfuzzy_page,
			CASE WHEN pageview_url = '/cart' THEN 1 ELSE 0 END AS cart_page
		FROM
			website_sessions
		LEFT JOIN
			website_pageviews
			ON website_sessions.website_session_id = website_pageviews.website_session_id
		WHERE
			website_sessions.created_at >= '2014-01-01' AND website_sessions.created_at <= '2014-02-01'
			AND website_pageviews.pageview_url IN('/lander-2','/products','/the-original-mr-fuzzy','/cart')
		ORDER BY
		website_sessions.website_session_id,
        website_pageviews.created_at
        ) AS pageview_level
GROUP BY
	website_session_id)
    
SELECT 
	COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN product_made_it = 1 THEN website_session_id ELSE NULL END)
    /COUNT(DISTINCT website_session_id)AS lander_clickthrough_rate,
    COUNT(DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN product_made_it = 1 THEN website_session_id ELSE NULL END) AS product_clickthrough_rate,
    COUNT(DISTINCT CASE WHEN cart_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN website_session_id ELSE NULL END) AS mrfuzzy_clickthrough_rate
FROM
	session_level_made_it_flags_demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_10

-- COMMAND ----------

WITH T1 AS (SELECT
	website_sessions.website_session_id,
     website_pageviews.pageview_url,
	website_pageviews.created_at AS pageview_created_at,
    CASE WHEN pageview_url = '/lander-1' THEN 1 ELSE 0 END AS lander_page,
    CASE WHEN pageview_url = '/products' THEN 1 ELSE 0 END AS product_page,
    CASE WHEN pageview_url = '/the-original-mr-fuzzy' THEN 1 ELSE 0 END AS mrfuzzy_page,
    CASE WHEN pageview_url = '/cart' THEN 1 ELSE 0 END AS cart_page,
    CASE WHEN pageview_url = '/shipping' THEN 1 ELSE 0 END AS shipping_page,
    CASE WHEN pageview_url = '/billing' THEN 1 ELSE 0 END AS billing_page,
    CASE WHEN pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END AS thankyou_page
FROM
	website_sessions
LEFT JOIN
	website_pageviews
    ON website_sessions.website_session_id = website_pageviews.website_session_id
WHERE
	website_sessions.created_at BETWEEN '2012-08-05' AND '2012-09-05'
    AND website_pageviews.pageview_url IN ('/lander-1','/products','/the-original-mr-fuzzy','/cart','/shipping','/billing','/thank-you-for-your-order')
ORDER BY
	website_sessions.website_session_id,
    website_pageviews.created_at),
T2 AS (
SELECT
	website_session_id,
    MAX(lander_page) AS lander_made_it,
    MAX(product_page) AS product_made_it,
    MAX(mrfuzzy_page) AS mrfuzzy_made_it,
    MAX(cart_page) AS cart_made_it,
    MAX(shipping_page) AS shipping_made_it,
    MAX(billing_page) AS billing_made_it,
    MAX(thankyou_page) AS thankyou_made_it
FROM 
	T1
GROUP BY
	website_session_id)
    
SELECT 
	COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN lander_made_it = 1 THEN website_session_id ELSE NULL END)  
    /COUNT(DISTINCT website_session_id) AS clicked_to_lander,
    COUNT(DISTINCT CASE WHEN product_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN lander_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_product,
    COUNT(DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN product_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_mrfuzzy,
    COUNT(DISTINCT CASE WHEN cart_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN mrfuzzy_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_cart,
    COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) 
	/COUNT(DISTINCT CASE WHEN cart_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_shipping,
    COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_billing,
    COUNT(DISTINCT CASE WHEN thankyou_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_thankyou
FROM
	T2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_11

-- COMMAND ----------


WITH T1 AS (
SELECT 
	website_sessions.website_session_id,
    website_pageviews.pageview_url,
    website_pageviews.created_at AS pageview_created_at,
    CASE WHEN pageview_url = '/shipping' THEN 1 ELSE 0 END AS shipping_page,
    CASE WHEN pageview_url = '/billing' THEN 1 ELSE 0 END AS billing_page,
    CASE WHEN pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END AS thankyou_page
FROM 
	website_sessions
LEFT JOIN
	website_pageviews
    ON website_sessions.website_session_id = website_pageviews.website_session_id
WHERE
	website_sessions.created_at < '2012-11-10'
    AND website_pageviews.pageview_url IN ('/shipping','/billing','/thank-you-for-your-order')
ORDER BY
	website_sessions.website_session_id,
    website_pageviews.created_at),
T2 AS (
SELECT 
	website_session_id,
	MAX(shipping_page) AS shipping_made_it,
    MAX(billing_page) AS billing_made_it,
    MAX(thankyou_page) AS thankyou_made_it
FROM
	T1
GROUP BY
	website_session_id)
    
SELECT 
	COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT website_session_id) AS clicked_to_shipping,
    COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_billing,
    COUNT(DISTINCT CASE WHEN thankyou_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_thankyou
FROM
	T2;
    
 WITH T3 AS   
(SELECT 
	website_sessions.website_session_id,
    website_pageviews.pageview_url,
    website_pageviews.created_at AS pageview_created_at,
    CASE WHEN pageview_url = '/shipping' THEN 1 ELSE 0 END AS shipping_page,
    CASE WHEN pageview_url = '/billing-2' THEN 1 ELSE 0 END AS billing_page,
    CASE WHEN pageview_url = '/thank-you-for-your-order' THEN 1 ELSE 0 END AS thankyou_page
FROM
	website_sessions
LEFT JOIN
	website_pageviews
	ON website_sessions.website_session_id = website_pageviews.website_session_id
WHERE 
	website_pageviews.created_at < '2012-11-10'
    AND website_pageviews.pageview_url IN('/shipping','/billing-2','/thank-you-for-your-order')
ORDER BY
	website_sessions.website_session_id,
    website_pageviews.created_at),

T4 AS (    
SELECT 
	website_session_id,
    MAX(shipping_page) AS shipping_made_it,
    MAX(billing_page) AS billing_made_it,
    MAX(thankyou_page) AS thankyou_made_it
FROM
	T3
GROUP BY
	website_session_id)
    
SELECT 
	COUNT(DISTINCT website_session_id) AS sessions,
    COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT website_session_id) AS clicked_to_shipping,
    COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN shipping_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_billing,
    COUNT(DISTINCT CASE WHEN thankyou_made_it = 1 THEN website_session_id ELSE NULL END) 
    /COUNT(DISTINCT CASE WHEN billing_made_it = 1 THEN website_session_id ELSE NULL END) AS clicked_to_thankyou	
FROM
	T4
SELECT
	

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query_12

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

-- MAGIC %md
-- MAGIC ## Query_13

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
-- MAGIC ## Query_14

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

-- MAGIC %md
-- MAGIC ## Query_15

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
-- MAGIC ## Query_16

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
-- MAGIC ## Query_17

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
-- MAGIC ## Query_18

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
-- MAGIC ## Query_19

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


