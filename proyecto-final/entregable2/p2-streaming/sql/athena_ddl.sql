-- Tablas Delta de P2 en Athena (Athena motor v3 lee Delta Lake de forma nativa).
-- Reemplace <LAKE_BUCKET> por el bucket del lago (el mismo de --lake_root) y ajuste la base de datos.
--
-- Para tablas Delta no se listan columnas ni particiones: Athena las toma del log de transacciones
-- (_delta_log), así que el DDL no se desactualiza si el esquema evoluciona. Lo único que hay que declarar es
-- table_type = 'DELTA'. Las particiones (ingest_date, event_date) se podan solas al filtrar por esa columna.
--
-- Importante para los consumidores (Superset, verificadores de P1):
--  * agg_sales_minute.category_id es NULL en la fila total (category = 'Total'). Use `category_id IS NULL`
--    o `category = 'Total'`; no `category_id = 0`.
--  * Los timestamps están en UTC. event_date es la fecha en hora de Colombia (UTC-5).

CREATE DATABASE IF NOT EXISTS tiendacol_lake;

-- Bronze: todo lo recibido (duplicados y malformados incluidos). Partición: ingest_date.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.bronze_events
LOCATION 's3://<LAKE_BUCKET>/bronze/events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Cuarentena: rechazados con su causa (reason). Partición: ingest_date.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.quarantine_events
LOCATION 's3://<LAKE_BUCKET>/quarantine/events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Silver: un evento válido, único y enriquecido por event_id. Partición: event_date.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.silver_events
LOCATION 's3://<LAKE_BUCKET>/silver/events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Stock actual por producto y bodega (RF-03): stock = base_stock + cumulative_delta.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.stock_current
LOCATION 's3://<LAKE_BUCKET>/silver/stock_current/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Alertas de agotamiento emitidas (RF-07).
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.alert_log
LOCATION 's3://<LAKE_BUCKET>/silver/alert_log/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Ventas por minuto y categoría (RF-09). La escribe el streaming (Q5); el resto de Gold lo escribe el batch.
CREATE EXTERNAL TABLE IF NOT EXISTS tiendacol_lake.agg_sales_minute
LOCATION 's3://<LAKE_BUCKET>/gold/agg_sales_minute/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- ---------------------------------------------------------------------------------------------
-- Consultas de comprobación
-- ---------------------------------------------------------------------------------------------

-- RNF-03: Bronze conserva los reenvíos, Silver no tiene duplicados (las dos diferencias deben ser 0 / 0).
SELECT (SELECT count(*) FROM tiendacol_lake.silver_events) - (SELECT count(DISTINCT event_id) FROM tiendacol_lake.silver_events) AS duplicados_en_silver,
       (SELECT count(*) FROM tiendacol_lake.bronze_events) - (SELECT count(*) FROM tiendacol_lake.silver_events)
                                                          - (SELECT count(*) FROM tiendacol_lake.quarantine_events) AS reenvios_en_bronze;

-- RF-05: causas de cuarentena.
SELECT split_part(reason, ':', 1) AS causa, count(*) AS eventos
FROM tiendacol_lake.quarantine_events GROUP BY 1 ORDER BY 2 DESC;

-- RF-09: ventas del último minuto (fila total y por categoría).
SELECT minute_start, category_id, category, orders, sales_cop
FROM tiendacol_lake.agg_sales_minute
WHERE minute_start >= date_add('hour', -1, current_timestamp)
ORDER BY minute_start DESC, category_id NULLS FIRST;

-- RF-03: stock por producto.
SELECT product_id, sum(stock) AS stock FROM tiendacol_lake.stock_current GROUP BY 1 ORDER BY 1 LIMIT 20;
