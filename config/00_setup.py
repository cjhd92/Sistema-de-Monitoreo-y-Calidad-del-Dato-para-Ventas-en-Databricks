# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS workspace.dbventas")
spark.sql("USE workspace.dbventas")


# COMMAND ----------

spark.sql("""
          create table if not exists meta_jobs(
              job_name string,
              layer string,
              start_time timestamp,
              end_time timestamp,
              rows long,
              id_ingesta_last int,
              message string
          )
          using delta
          """)

# COMMAND ----------

spark.sql("""
          create table if not exists logs_job(
              log_time timestamp,
              job_name string,
              layer string,
              level string,  -- INF,WARN,ERROR
              message string
          )
          using delta
             
          """)