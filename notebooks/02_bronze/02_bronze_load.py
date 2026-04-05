# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "./utiles_funtion"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze incremental

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col


layer = "Bronze"

# COMMAND ----------

job_name = "bron_customers"


try:
  bronze_table_customers = "workspace.dbventas.bronze_customers"
  landing_table_customers = "workspace.dbventas.landing_customers"

  df_landing_customers = spark.read.table(landing_table_customers)


  # Si Bronze existe, cargarlo
  if spark.catalog.tableExists(bronze_table_customers):
    
    df_bronze_customers = spark.read.table(bronze_table_customers)
    df_new_customers = df_landing_customers.join(df_bronze_customers, on='id_ingestion', how='left_anti')

  else:
    df_new_customers = df_landing_customers

  # Insertar solo los nuevos
  df_new_customers.write.format("delta").mode("append").saveAsTable(bronze_table_customers)

  row_count = df_new_customers.count()

  last_id = df_new_customers.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None


  log_meta(job_name, layer, row_count, last_id, "SUCCESS")
  log_event(job_name, layer, "INFO", f"{row_count} filas nuevas insertadas en Bronze.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

job_name = "bron_orders"


try:
  bronze_table_orders = "workspace.dbventas.bronze_orders"
  landing_table_orders = "workspace.dbventas.landing_orders"

  df_landing_orders = spark.read.table(landing_table_orders)


  # Si Bronze existe, cargarlo
  if spark.catalog.tableExists(bronze_table_orders):
    
    df_bronze_orders = spark.read.table(bronze_table_orders)
    df_new_orders = df_landing_orders.join(df_bronze_orders, on='id_ingestion', how='left_anti')

  else:
    df_new_orders = df_landing_orders

  # Insertar solo los nuevos
  df_new_orders.write.format("delta").mode("append").saveAsTable(bronze_table_orders)

  row_count = df_new_orders.count()

  last_id = df_new_orders.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None


  log_meta(job_name, layer, row_count, last_id, "SUCCESS")
  log_event(job_name, layer, "INFO", f"{row_count} filas nuevas insertadas en Bronze.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

job_name = "bron_products"


try:
  bronze_table_products = "workspace.dbventas.bronze_products"
  landing_table_products = "workspace.dbventas.landing_products"

  df_landing_products = spark.read.table(landing_table_products)


  # Si Bronze existe, cargarlo
  if spark.catalog.tableExists(bronze_table_products):
    
    df_bronze_products = spark.read.table(bronze_table_products)
    df_new_products = df_landing_products.join(df_bronze_products, on='id_ingestion', how='left_anti')

  else:
    df_new_products = df_landing_products

  # Insertar solo los nuevos
  df_new_products.write.format("delta").mode("append").saveAsTable(bronze_table_products)

  row_count = df_new_products.count()

  last_id = df_new_products.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None


  log_meta(job_name, layer, row_count, last_id, "SUCCESS")
  log_event(job_name, layer, "INFO", f"{row_count} filas nuevas insertadas en Bronze.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e