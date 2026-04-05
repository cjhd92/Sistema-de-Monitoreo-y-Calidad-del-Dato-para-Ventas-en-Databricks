# Databricks notebook source
# MAGIC %md
# MAGIC **Reconocer las funciones de meta y log del archivo utils**

# COMMAND ----------

# MAGIC %run "./utiles_funtion"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calidad de datos

# COMMAND ----------


from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, lit, current_timestamp, to_json, struct


layer = "Quality"




# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema_rejected = StructType([
    StructField("table_name", StringType(), True),
    StructField("rule_id", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("raw_record", StringType(), True),
    StructField("timestamp", StringType(), True)
])

spark.createDataFrame([], schema_rejected) \
     .write.mode("overwrite") \
     .saveAsTable("workspace.dbventas.rejected_records")


# COMMAND ----------

# DBTITLE 1,Cell 5
# Verificamos la calidad de los datos,segun la regla de calidad del proyecto:
# name NULL
# email NULL
# country no puede tener valores no esperados

job_name = "qua_customers"


try:
    silver_table = "workspace.dbventas.silver_customers"
    valid_table  = "workspace.dbventas.quality_valid_customers"

    df_silver = spark.table(silver_table)


    # Detectar solo registros nuevos respecto a los ya validados
    if spark.catalog.tableExists(valid_table):

        df_valid = spark.table(valid_table)
        df_new = df_silver.join(df_valid.select("customer_id"), "customer_id", "left_anti")
    else:
        df_new = df_silver

    rows_checked = df_new.count()

    if rows_checked == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos customers para validar.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        # Partimos de df_new como candidatos válidos
        df_valid_new = df_new

        # ===== Regla 1: name NOT NULL =====
        df_failed_1 = df_valid_new.filter(col("name").isNull())
        df_valid_new = df_valid_new.filter(col("name").isNotNull())

        rows_failed = df_failed_1.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(1, "silver_customers", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_1.withColumn("table_name", lit("customers")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("name is NULL")) \
                   .withColumn("rule_id", lit("1")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # ===== Regla 2: email NOT NULL =====
        df_failed_2 = df_valid_new.filter(col("email").isNull())
        df_valid_new = df_valid_new.filter(col("email").isNotNull())

        rows_failed = df_failed_2.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(2, "silver_customers", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_2.withColumn("table_name", lit("customers")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("email is NULL")) \
                   .withColumn("rule_id", lit("2")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # ===== Regla 3: country DOMAIN =====
        valid_countries = validate_country()

        df_failed_3 = df_valid_new.filter(~col("country").isin(valid_countries))
        df_valid_new = df_valid_new.filter(col("country").isin(valid_countries))

        rows_failed = df_failed_3.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(3, "silver_customers", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_3.withColumn("table_name", lit("customers")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("country not in domain")) \
                   .withColumn("rule_id", lit("3")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # Guardar solo los válidos nuevos en tabla de válidos
        write_mode = "append" if spark.catalog.tableExists(valid_table) else "overwrite"
        df_valid_new.write.mode(write_mode).saveAsTable(valid_table)

        log_event(job_name, layer, "INFO", f"{df_valid_new.count()} customers válidos nuevos.")
        log_meta(job_name, layer, rows_checked, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

# Verificamos la calidad de los datos,segun la regla de calidad del proyecto:
# price > 0
# category  no puede tener valores no esperados

job_name = "qua_products"


try:
    silver_table = "workspace.dbventas.silver_products"
    valid_table  = "workspace.dbventas.quality_valid_products"

    df_silver = spark.table(silver_table)


    # Detectar solo registros nuevos respecto a los ya validados
    if spark.catalog.tableExists(valid_table):

        df_valid = spark.table(valid_table)
        df_new = df_silver.join(df_valid.select("product_id"), "product_id", "left_anti")
    else:
        df_new = df_silver

    rows_checked = df_new.count()

    if rows_checked == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos products para validar.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        # Partimos de df_new como candidatos válidos
        df_valid_new = df_new

        # ===== Regla 4: price <= 0 =====
        df_failed_4 = df_valid_new.filter(col("price") <= 0)
        df_valid_new = df_valid_new.filter(col("price") > 0)

        rows_failed = df_failed_4.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(4, "silver_products", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_4.withColumn("table_name", lit("products")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("price <= 0")) \
                   .withColumn("rule_id", lit("4")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")


        # ===== Regla 5: category DOMAIN =====
        valid_countries = validate_category()

        df_failed_5 = df_valid_new.filter(~col("category").isin(valid_countries))
        df_valid_new = df_valid_new.filter(col("category").isin(valid_countries))

        rows_failed = df_failed_5.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(5, "silver_products", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_5.withColumn("table_name", lit("customers")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("country not in domain")) \
                   .withColumn("rule_id", lit("5")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # Guardar solo los válidos nuevos en tabla de válidos
        write_mode = "append" if spark.catalog.tableExists(valid_table) else "overwrite"
        df_valid_new.write.mode(write_mode).saveAsTable(valid_table)

        log_event(job_name, layer, "INFO", f"{df_valid_new.count()} products válidos nuevos.")
        log_meta(job_name, layer, rows_checked, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e

# COMMAND ----------

# DBTITLE 1,Cell 8
# Verificamos la calidad de los datos,segun la regla de calidad del proyecto:
# total_amount  > 0
# quantity > 0
# customer_id existe en silver_customers
# product_id existe en silver_products

job_name = "qua_orders"


try:
    silver_table = "workspace.dbventas.silver_orders"
    valid_table  = "workspace.dbventas.quality_valid_orders"

    df_silver = spark.table(silver_table)

    df_customers = spark.table("workspace.dbventas.quality_valid_customers")
    df_products  = spark.table("workspace.dbventas.quality_valid_products")


    # Detectar solo registros nuevos respecto a los ya validados
    if spark.catalog.tableExists(valid_table):

        df_valid = spark.table(valid_table)
        df_new = df_silver.join(df_valid.select("order_id"), "order_id", "left_anti")
    else:
        df_new = df_silver

    rows_checked = df_new.count()

    if rows_checked == 0:
        log_event(job_name, layer, "INFO", "No hay nuevos orders para validar.")
        log_meta(job_name, layer, 0, None, "SUCCESS")
    else:
        # Partimos de df_new como candidatos válidos
        df_valid_new = df_new

        # ===== Regla 6: total_amount <= 0 =====
        df_failed_6 = df_valid_new.filter(col("total_amount") <= 0)
        df_valid_new = df_valid_new.filter(col("total_amount") > 0)

        rows_failed = df_failed_6.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(6, "silver_orders", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_6.withColumn("table_name", lit("orders")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("total_amount <= 0")) \
                   .withColumn("rule_id", lit("6")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")


        # ===== Regla 7: quantity > 0 =====
        df_failed_7 = df_valid_new.filter(col("quantity") <= 0)
        df_valid_new = df_valid_new.filter(col("quantity") > 0)

        rows_failed = df_failed_7.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(7, "silver_orders", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_7.withColumn("table_name", lit("orders")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("quantity <= 0")) \
                   .withColumn("rule_id", lit("7")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # ===== Regla 8: FK customer_id =====
        df_failed_8 = df_valid_new.join(df_customers.select("customer_id"), "customer_id", "left_anti")
        df_valid_new = df_valid_new.join(df_customers.select("customer_id"), "customer_id", "inner")

        rows_failed = df_failed_8.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(8, "silver_orders", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")

        df_failed_8.withColumn("table_name", lit("orders")) \
                   .withColumn("raw_record", to_json(struct("*"))) \
                   .withColumn("timestamp", current_timestamp().cast("string")) \
                   .withColumn("reason", lit("customer_id not found in valid customers")) \
                   .withColumn("rule_id", lit("8")) \
                   .select("table_name","rule_id","reason","raw_record","timestamp") \
                   .write.mode("append").saveAsTable("workspace.dbventas.rejected_records")

        # Regla 9: FK product_id
        df_failed_9 = df_valid_new.join(df_products.select("product_id"), "product_id", "left_anti")
        df_valid_new = df_valid_new.join(df_products.select("product_id"), "product_id", "inner")

        rows_failed = df_failed_9.count()
        pass_rate = (rows_checked - rows_failed) / rows_checked

        spark.createDataFrame([(9, "silver_orders", rows_checked, rows_failed, pass_rate)]) \
            .toDF("rule_id","table_name","rows_checked","rows_failed","pass_rate") \
            .withColumn("run_time", current_timestamp()) \
            .write.mode("append").saveAsTable("workspace.dbventas.quality_results")


        # Guardar solo los válidos nuevos en tabla de válidos
        write_mode = "append" if spark.catalog.tableExists(valid_table) else "overwrite"
        df_valid_new.write.mode(write_mode).saveAsTable(valid_table)

        log_event(job_name, layer, "INFO", f"{df_valid_new.count()} orders válidos nuevos.")
        log_meta(job_name, layer, rows_checked, None, "SUCCESS")

except Exception as e:
    log_event(job_name, layer, "ERROR", str(e))
    log_meta(job_name, layer, -1, None, "ERROR")
    raise e