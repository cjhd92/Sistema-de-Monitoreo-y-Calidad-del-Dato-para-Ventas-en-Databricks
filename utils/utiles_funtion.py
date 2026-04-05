# Databricks notebook source
# MAGIC %md
# MAGIC Funciones utiles  
# MAGIC `log_meta`, `log_event`, `validate_country` , `validate_category`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

def log_meta(job_name, layer, row_count, id_ingesta_last, message=None):
    (
        spark.createDataFrame(
            [(job_name, layer)],
            "job_name STRING, layer STRING"
        )
        .withColumn("start_time", current_timestamp())
        .withColumn("end_time", current_timestamp())
        .withColumn("rows", lit(row_count).cast("long"))
        .withColumn("id_ingesta_last", lit(id_ingesta_last))
        .withColumn("message", lit(message))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.dbventas.meta_jobs")   # ← CORREGIDO
    )

# COMMAND ----------

def log_event(job_name, layer, level, message):
    (
        spark.createDataFrame(
            [(job_name, layer, level, message)],
            "job_name STRING, layer STRING, level STRING, message STRING"
        )
            .withColumn("log_time", current_timestamp()) 
            .select("log_time", "job_name", "layer", "level", "message") 
            .write
            .format("delta")
            .mode("append")
            .saveAsTable("workspace.dbventas.logs_job") 
    )

# COMMAND ----------

#Funcion que se usa en la capa de calidad de datos, especificamente en la tabla customers, la idea es validad cuales son los paises que se utiliza y se quiere modificar , añadir o eliminar, pues basta con modificar solo la funcion.

def validate_country():
    lista_valid_countries = ["ES", "US", "FR"]
    
    return lista_valid_countries

# COMMAND ----------

#Funcion que se usa en la capa de calidad de datos, especificamente en la tabla products, la idea es validad cuales son las categorias de productos que se utiliza y se quiere modificar , añadir o eliminar, pues basta con modificar solo la funcion.

def validate_category():
    lista_validate_category = ["Electronics", "Furniture"]
    
    return lista_validate_category