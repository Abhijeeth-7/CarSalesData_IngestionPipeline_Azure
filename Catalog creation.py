# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE Catalog carSales_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema carSales_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema carSales_catalog.gold
