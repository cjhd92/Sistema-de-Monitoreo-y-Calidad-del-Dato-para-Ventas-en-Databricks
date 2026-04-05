# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "./utiles_funtion"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold es modelo analítico

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

layer = "Gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customers

# COMMAND ----------

job_name = "gold_dim_customers"


try:
    source_table = "workspace.dbventas.quality_valid_customers"
    gold_table   = "workspace.dbventas.dim_customers"

    df_source = spark.table(source_table)

    # Detectar incremental
    if spark.catalog.tableExists(gold_table):
        df_gold = spark.table(gold_table)
        df_new = df_source.join(df_gold.select("customer_id"), "customer_id", "left_anti")
    else:
        df_new = df_source

    rows_new = df_new.count()

    if rows_new == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos customers para GOLD.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        df_dim = df_new.select(
            "customer_id",
            "name",
            "email",
            "country"
        ).dropDuplicates(["customer_id"])

        write_mode = "append" if spark.catalog.tableExists(gold_table) else "overwrite"
        df_dim.write.mode(write_mode).saveAsTable(gold_table)

        log_event(job_name, layer, "INFO", f"{rows_new} nuevos customers añadidos a dim_customers.")
        log_meta(job_name, layer, rows_new, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_products

# COMMAND ----------


job_name = "gold_dim_products"


try:
    source_table = "workspace.dbventas.quality_valid_products"
    gold_table   = "workspace.dbventas.dim_products"

    df_source = spark.table(source_table)

    # Incremental
    if spark.catalog.tableExists(gold_table):
        df_gold = spark.table(gold_table)
        df_new = df_source.join(df_gold.select("product_id"), "product_id", "left_anti")
    else:
        df_new = df_source

    rows_new = df_new.count()

    if rows_new == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos products para GOLD.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        df_dim = df_new.select(
            "product_id",
            "product_name",
            "category",
            "price"
        ).dropDuplicates(["product_id"])

        write_mode = "append" if spark.catalog.tableExists(gold_table) else "overwrite"
        df_dim.write.mode(write_mode).saveAsTable(gold_table)

        log_event(job_name, layer, "INFO", f"{rows_new} nuevos products añadidos a dim_products.")
        log_meta(job_name, layer, rows_new, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e


# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_orders

# COMMAND ----------


job_name = "gold_fact_orders"


try:
    source_table = "workspace.dbventas.quality_valid_orders"
    gold_table   = "workspace.dbventas.fact_orders"

    df_source = spark.table(source_table)

    # Incremental
    if spark.catalog.tableExists(gold_table):
        df_gold = spark.table(gold_table)
        df_new = df_source.join(df_gold.select("order_id"), "order_id", "left_anti")
    else:
        df_new = df_source

    rows_new = df_new.count()

    if rows_new == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos orders para GOLD.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        df_fact = df_new.select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "total_amount"
        ).dropDuplicates(["order_id"])

        write_mode = "append" if spark.catalog.tableExists(gold_table) else "overwrite"
        df_fact.write.mode(write_mode).saveAsTable(gold_table)

        log_event(job_name, layer, "INFO", f"{rows_new} nuevos orders añadidos a fact_orders.")
        log_meta(job_name, layer, rows_new, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e
