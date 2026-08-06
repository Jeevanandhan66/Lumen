-- Databricks notebook source
-- MAGIC %python
-- MAGIC df = (
-- MAGIC     spark.read
-- MAGIC          .option("header", "true")
-- MAGIC          .option("inferSchema", "true")
-- MAGIC          .csv("/Volumes/dev_landing_dlh_finance/finsight/stage/shared_dim_date.csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write \
-- MAGIC   .format("delta") \
-- MAGIC   .mode("overwrite") \
-- MAGIC   .saveAsTable("dev_udl_dlh_finance.finsight.sa_staging_shared_dim_date")

-- COMMAND ----------

select count(*) from dev_udl_dlh_finance.finsight.sa_staging_shared_dim_date