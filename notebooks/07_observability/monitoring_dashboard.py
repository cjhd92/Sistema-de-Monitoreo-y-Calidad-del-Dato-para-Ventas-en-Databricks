# Databricks notebook source
# MAGIC %md
# MAGIC **Resumen de calidad**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.dbventas.vw_quality_summary AS
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     rows_checked,
# MAGIC     rows_failed,
# MAGIC     pass_rate,
# MAGIC     run_time
# MAGIC FROM workspace.dbventas.quality_results
# MAGIC ORDER BY run_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Errores de jobs**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.dbventas.vw_job_errors AS
# MAGIC SELECT 
# MAGIC     job_name,
# MAGIC     layer,
# MAGIC     message,
# MAGIC     log_time
# MAGIC FROM workspace.dbventas.logs_job
# MAGIC WHERE level = 'ERROR'
# MAGIC ORDER BY log_time DESC;
# MAGIC