# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "./utiles_funtion"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesta a Landing

# COMMAND ----------

csv_customers = '/Workspace/proyectos/Sistema de Monitoreo y Calidad en Ventas/csv crudos/customers.csv'
csv_orders = '/Workspace/proyectos/Sistema de Monitoreo y Calidad en Ventas/csv crudos/orders.csv'
csv_products = '/Workspace/proyectos/Sistema de Monitoreo y Calidad en Ventas/csv crudos/products.csv'

layer = 'Landing'

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp

job_name = 'lan_custumers'

try:
    df_raw_custumers = spark.read.format('csv').option('header',True).csv(csv_customers)

    df_landing_custumers = (df_raw_custumers
                            .withColumn('id_ingestion', monotonically_increasing_id())
                            .withColumn('timestamp_ingestion', current_timestamp())
                            )

    df_landing_custumers.write.format('delta').mode('overwrite').saveAsTable('workspace.dbventas.landing_customers')

    #enrriqueza la tabla de metadata
    row_count = df_landing_custumers.count()
    last_id = df_landing_custumers.agg({"id_ingestion": "max"}).collect()[0][0]

    log_meta(job_name, layer,row_count,last_id,"SUCCESS")
    log_event(job_name, layer, "INFO", "Ingesta completada correctamente.")



except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, current_timestamp

job_name = 'lan_orders'

try:
    df_raw_orders = spark.read.format('csv').option('header',True).csv(csv_orders)

    df_landing_orders = (df_raw_orders
                            .withColumn('id_ingestion', monotonically_increasing_id())
                            .withColumn('timestamp_ingestion', current_timestamp())
                            )

    df_landing_orders.write.format('delta').mode('overwrite').saveAsTable('workspace.dbventas.landing_orders')

    #enrriqueza la tabla de metadata
    row_count = df_landing_orders.count()
    last_id = df_landing_orders.agg({"id_ingestion": "max"}).collect()[0][0]

    log_meta(job_name, layer,row_count,last_id,"SUCCESS")
    log_event(job_name, layer, "INFO", "Ingesta completada correctamente.")



except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, current_timestamp

job_name = 'lan_products'

try:
    df_raw_products = spark.read.format('csv').option('header',True).csv(csv_products)

    df_landing_products = (df_raw_products
                            .withColumn('id_ingestion', monotonically_increasing_id())
                            .withColumn('timestamp_ingestion', current_timestamp())
                            )

    df_landing_products.write.format('delta').mode('overwrite').saveAsTable('workspace.dbventas.landing_products')

    #enrriqueza la tabla de metadata
    row_count = df_landing_products.count()
    last_id = df_landing_products.agg({"id_ingestion": "max"}).collect()[0][0]

    log_meta(job_name, layer,row_count,last_id,"SUCCESS")
    log_event(job_name, layer, "INFO", "Ingesta completada correctamente.")



except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e