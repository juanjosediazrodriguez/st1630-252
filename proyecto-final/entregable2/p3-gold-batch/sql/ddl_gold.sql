-- =============================================================================
-- ST1630 · Sistemas Intensivos en Datos · TiendaCol en Tiempo Real (E2)
-- Responsable P3: Juan José Díaz Rodríguez
-- Archivo: sql/ddl_gold.sql
-- Propósito: Registro de las tablas Delta Lake de Gold en AWS Glue Data Catalog
--            para consulta directa con Amazon Athena (motor v3) y Apache Superset.
--
-- Nota técnica (Athena v3 + Delta Lake):
-- Para tablas con TBLPROPERTIES ('table_type' = 'DELTA') no se declaran columnas
-- ni particiones en el CREATE EXTERNAL TABLE: Athena lee el esquema y las
-- particiones directamente desde el _delta_log de S3. Así el DDL nunca queda
-- desfasado frente a cambios de esquema.
--
-- Reemplace <LAKE_BUCKET> por el nombre real del bucket S3 del lago.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tiendacol_gold;

-- =============================================================================
-- 1. PUNTAS DE LA ESTRELLA: TABLAS DE DIMENSIONES (dim_*)
-- =============================================================================

-- 1.1 Dimensión de Productos con SCD Tipo 2 (product_key, product_id, name,
--     category_id, category, seller_id, price, attributes, valid_from, valid_to, is_current)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_product
LOCATION 's3://<LAKE_BUCKET>/gold/dim_product/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 1.2 Dimensión de Vendedores (seller_key, seller_id, seller_name, city, department)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_seller
LOCATION 's3://<LAKE_BUCKET>/gold/dim_seller/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 1.3 Dimensión de Clientes seudonimizados (customer_key, customer_id, city, department, created_at)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_customer
LOCATION 's3://<LAKE_BUCKET>/gold/dim_customer/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 1.4 Dimensión de Ubicación Geográfica (location_key, city, department)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_location
LOCATION 's3://<LAKE_BUCKET>/gold/dim_location/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 1.5 Dimensión de Dispositivos (device_key, device_name)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_device
LOCATION 's3://<LAKE_BUCKET>/gold/dim_device/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 1.6 Dimensión de Tiempo a nivel de minuto (datetime_key, minute_ts, date, hour, year, month, day, minute)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.dim_datetime
LOCATION 's3://<LAKE_BUCKET>/gold/dim_datetime/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- =============================================================================
-- 2. CENTROS DE LA ESTRELLA: TABLAS DE HECHOS (fact_*) Y AGREGADOS (agg_*)
-- =============================================================================

-- 2.1 Tabla de Hechos del Embudo de Conversión (RF-10 + trazabilidad RNF-06)
--     Particionada por event_date.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.fact_funnel_event
LOCATION 's3://<LAKE_BUCKET>/gold/fact_funnel_event/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 2.2 Tabla de Hechos de Ventas (RF-11 + trazabilidad RNF-06)
--     Grano: 1 fila por línea de orden (order_id + line_number). Particionada por event_date.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.fact_sales
LOCATION 's3://<LAKE_BUCKET>/gold/fact_sales/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 2.3 Tabla Agregada de Afinidad de Productos / Cesta (RF-13: comprados juntos)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.agg_product_affinity
LOCATION 's3://<LAKE_BUCKET>/gold/agg_product_affinity/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- 2.4 Tabla de Ventas por Minuto (RF-09: escrita por Q5 de Glue Streaming, expuesta en tiendacol_gold para Superset)
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_gold.agg_sales_minute
LOCATION 's3://<LAKE_BUCKET>/gold/agg_sales_minute/'
TBLPROPERTIES ('table_type' = 'DELTA');
