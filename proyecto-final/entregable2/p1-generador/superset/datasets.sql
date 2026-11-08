-- Datasets de Superset (cada bloque = un dataset virtual / un gráfico).
-- Conexión:  awsathena+rest://@athena.us-east-1.amazonaws.com:443/tiendacol_gold?s3_staging_dir=s3://<BUCKET-OPS>/athena-results/&work_group=primary
--
-- !! Los nombres de tablas y columnas son la PROPUESTA de P1 a partir de la tabla de grano del E1 (ADR-3).
--    Ajustarlos al DDL definitivo de P3 en la Fase 0. Si P3 cambia algo, solo se edita este archivo.

-- 1. RF-09 · Ventas por minuto (total) — gráfico de línea, últimos 60 min, refresco 30 s
--    Reglas: la fila total tiene category_id NULL.
SELECT minute_start, orders, sales_cop
FROM agg_sales_minute
WHERE category_id IS NULL
  AND minute_start >= date_add('minute', -60, current_timestamp)
ORDER BY minute_start;

-- 2. RF-09 · Ventas por minuto por categoría — barras apiladas
SELECT a.minute_start, c.category_name, a.orders, a.sales_cop
FROM agg_sales_minute a
JOIN (SELECT DISTINCT category_id, category AS category_name FROM dim_product WHERE is_current) c
  ON c.category_id = a.category_id
WHERE a.minute_start >= date_add('minute', -60, current_timestamp)
ORDER BY a.minute_start;

-- 3. RF-10 · Embudo producto → carrito → compra, por categoría / dispositivo / ciudad / hora
--    Sesiones DISTINTAS por etapa (no eventos). Conversión = etapa siguiente / etapa anterior.
WITH s AS (
  SELECT f.session_id, p.category AS category, d.device_name AS device, l.city,
         date_trunc('hour', t.minute_ts) AS hour,
         max(CASE WHEN f.stage = 'page_view'   THEN 1 ELSE 0 END) AS viewed,
         max(CASE WHEN f.stage = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
         max(CASE WHEN f.stage = 'purchase'    THEN 1 ELSE 0 END) AS bought
  FROM fact_funnel_event f
  JOIN dim_product  p ON p.product_key  = f.product_key
  JOIN dim_device   d ON d.device_key   = f.device_key
  JOIN dim_location l ON l.location_key = f.location_key
  JOIN dim_datetime t ON t.datetime_key = f.datetime_key
  GROUP BY 1, 2, 3, 4, 5
)
SELECT category, device, city, hour,
       sum(viewed) AS views, sum(carted) AS carts, sum(bought) AS purchases,
       try(1.0 * sum(carted) / nullif(sum(viewed), 0)) AS view_to_cart,
       try(1.0 * sum(bought) / nullif(sum(carted), 0)) AS cart_to_purchase
FROM s GROUP BY 1, 2, 3, 4;

-- 4. RF-11 · Ventas, órdenes y ticket promedio por categoría / vendedor / región / fecha
--    Ticket = ventas / órdenes (nunca promedio de promedios).
SELECT t.date AS date, p.category, s.seller_name, l.department AS region,
       sum(f.net_value) AS sales_cop,
       count(DISTINCT f.order_id) AS orders,
       sum(f.net_value) / count(DISTINCT f.order_id) AS avg_ticket
FROM fact_sales f
JOIN dim_product  p ON p.product_key  = f.product_key
JOIN dim_seller   s ON s.seller_key   = f.seller_key
JOIN dim_location l ON l.location_key = f.location_key
JOIN dim_datetime t ON t.datetime_key = f.datetime_key
GROUP BY 1, 2, 3, 4;

-- 5. Frescura analítica (RF-10/11: <= 1 h) — KPI: minutos desde el último dato en Gold
SELECT date_diff('minute', max(t.minute_ts), current_timestamp) AS minutes_since_last_fact
FROM fact_sales f JOIN dim_datetime t ON t.datetime_key = f.datetime_key;
