# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "../utiles_funtion"

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

job_name = "alert_quality_customers"
layer = "Observability"

try:
    df = spark.table("workspace.dbventas.quality_results") \
              .filter(col("table_name") == "silver_customers") \
              .orderBy(col("run_time").desc()) \
              .limit(1)

    pass_rate = df.collect()[0]["pass_rate"]

    if pass_rate < 0.95:
        alert_msg = f"ALERTA: La calidad de customers cayó a {pass_rate:.2%}"

        spark.createDataFrame([(alert_msg, current_timestamp())], ["alert_message","timestamp"]) \
             .write.mode("append").saveAsTable("workspace.dbventas.alerts")

        log_event(job_name, layer, "ERROR", alert_msg)
    else:
        log_event(job_name, layer, "INFO", f"Calidad OK: {pass_rate:.2%}")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    raise e
