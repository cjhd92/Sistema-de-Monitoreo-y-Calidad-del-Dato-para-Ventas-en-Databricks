# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "./utiles_funtion"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver (Data Lake limpio)

# COMMAND ----------

from pyspark.sql.functions import trim, upper, lower, col,initcap,to_date

layer = "Silver"



# COMMAND ----------


bronze_table_customers = 'workspace.dbventas.bronze_customers'
job_name = "sil_customers"


try:

    
    silver_table_customers = 'workspace.dbventas.silver_customers'

    df_bronze_customers = spark.read.table(bronze_table_customers)

    if spark.catalog.tableExists(silver_table_customers):
    
        df_bronze_customers = spark.read.table(bronze_table_customers)
        df_silver_customers = spark.table(silver_table_customers)

        df_new_custumers = df_bronze_customers.join(df_silver_customers, on='id_ingestion', how='left_anti')

    else:
        
        df_new_custumers = df_bronze_customers


    
    df_silver_new_custumers = (df_new_custumers
        .withColumn("name", trim(col("name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("country", upper(trim(col("country"))))
    )

    

    df_silver_new_custumers.write.format("delta").mode("append").saveAsTable("workspace.dbventas.silver_customers")
    
    row_count = df_silver_new_custumers.count()
    last_id = df_silver_new_custumers.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None

    log_meta(job_name, layer, row_count,last_id, "SUCCESS")
    log_event(job_name, layer, "INFO", "Silver customers generado correctamente.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e



# COMMAND ----------

# DBTITLE 1,Cell 7

# Transformacion y limpieza dedicada a la capa de silver y crea la tabla silver_products.Los datos provienen de la tabla bronze_products

job_name = "sil_products"
bronze_table = 'workspace.dbventas.bronze_products'
silver_table = 'workspace.dbventas.silver_products'


try:

    df_bronze = spark.read.table(bronze_table)

    if spark.catalog.tableExists(silver_table):
    
        df_bronze = spark.read.table(bronze_table)
        df_silver = spark.table(silver_table)

        df_new = df_bronze.join(df_silver, on='id_ingestion', how='left_anti')

    else:
        
        df_new = df_bronze

    df_silver_new = (df_new
        .withColumn("product_name", initcap(trim(col("product_name"))))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("price", col("price").cast("double"))
    )

    df_silver_new.write.format("delta").mode("append").saveAsTable(silver_table)
    
    row_count = df_silver_new.count()
    last_id = df_silver_new.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None

    log_meta(job_name, layer, row_count,last_id, "SUCCESS")
    log_event(job_name, layer, "INFO", "Silver products generado correctamente.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

# Transformacion y limpieza dedicada a la capa de silver y crea la tabla silver_orders.Los datos provienen de la tabla bronze_orders

job_name = "sil_orders"
bronze_table = 'workspace.dbventas.bronze_orders'
silver_table = 'workspace.dbventas.silver_orders'


try:

    df_bronze = spark.read.table(bronze_table)

    if spark.catalog.tableExists(silver_table):
    
        df_bronze = spark.read.table(bronze_table)
        df_silver = spark.table(silver_table)

        df_new = df_bronze.join(df_silver, on='id_ingestion', how='left_anti')

    else:
        
        df_new = df_bronze

    df_silver_new = (df_new
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("total_amount", col("total_amount").cast("double"))
    )

    df_silver_new.write.format("delta").mode("append").saveAsTable(silver_table)
    
    row_count = df_silver_new.count()
    last_id = df_silver_new.agg({"id_ingestion": "max"}).collect()[0][0] if row_count > 0 else None

    log_meta(job_name, layer, row_count,last_id, "SUCCESS")
    log_event(job_name, layer, "INFO", "Silver orders generado correctamente.")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e