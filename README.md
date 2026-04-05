# Sistema-de-Monitoreo-y-Calidad-del-Dato-para-Ventas-en-Databricks

Este proyecto implementa un pipeline de datos completo siguiendo la arquitectura Medallion (Bronze → Silver → Gold) sobre Databricks, incorporando:

Ingesta incremental

Limpieza y estandarización

Reglas de calidad del dato

Modelo analítico (dimensiones y hechos)

Vistas de consumo para BI

Observabilidad, alertas y monitoreo

Objetivos del proyecto
Construir un pipeline incremental, eficiente y escalable.

Implementar reglas de calidad del dato con trazabilidad.

Crear un modelo analítico listo para consumo.

Añadir observabilidad para monitorear la salud del pipeline.

Demostrar habilidades de arquitectura, ingeniería y diseño de datos.


✔️ Landing
Datos crudos tal como llegan.

✔️ Bronze
Ingesta incremental + estructura mínima.

✔️ Silver
Limpieza, normalización y tipado.

✔️ Quality
Reglas de calidad del dato:

NOT NULL

dominios válidos

integridad referencial

métricas de calidad


✔️ Gold
Modelo analítico:

dim_customers

dim_products

fact_orders

✔️ Consumption
Vistas analíticas listas para BI:

ventas por país


✔️ Observability
Dashboard + alertas:

calidad del dato

errores de jobs

alertas automáticas


🧑‍💻 Autor
César — Data Engineer  
A Coruña, España
