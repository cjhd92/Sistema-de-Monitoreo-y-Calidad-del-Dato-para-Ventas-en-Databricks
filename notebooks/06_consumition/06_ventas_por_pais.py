# Databricks notebook source
spark.sql("""
CREATE OR REPLACE VIEW workspace.dbventas.vw_ventas_por_pais AS
SELECT 
    c.country,
    SUM(f.total_amount) AS total_ventas,
    COUNT(f.order_id) AS numero_pedidos,
    COUNT(DISTINCT f.customer_id) AS clientes_unicos
FROM workspace.dbventas.fact_orders f
LEFT JOIN workspace.dbventas.dim_customers c
    ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_ventas DESC
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.dbventas.vw_ventas_por_pais;
# MAGIC