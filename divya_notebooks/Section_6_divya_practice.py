# Databricks notebook source
import pandas as pd

# COMMAND ----------

transactions = pd.read_csv('/Volumes/dbx_catalog/default/sample_files/project_transactions.csv')

# COMMAND ----------

transactions

# COMMAND ----------

# Group by 'PRODUCT_ID' and sum 'SALES_VALUE'
product_sales = transactions.groupby('PRODUCT_ID')['SALES_VALUE'].sum().reset_index()

# Plot the bar chart
product_sales.plot.bar(x='PRODUCT_ID', y='SALES_VALUE')

# COMMAND ----------


