-- =============================================================================
-- ST1630 · Sistemas Intensivos en Datos · TiendaCol en Tiempo Real (E2)
-- Responsable P3: Juan José Díaz Rodríguez
-- Archivo: consultas_athena.sql
-- Propósito: Consultas analíticas en Amazon Athena sobre el esquema estrella de
--            Gold para responder RF-10, RF-11, RF-13, SCD2 y RNF-06.
-- Compatibles 100% con Apache Superset (datasets.sql) y con el oráculo verify.py.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. RF-10 · EMBUDO DE CONVERSIÓN (ver producto -> carrito -> compra)
-- Cuenta sesiones DISTINTAS por etapa, desglosado por categoría, dispositivo,
-- ciudad y hora. Calcula las tasas de conversión entre etapas.
-- -----------------------------------------------------------------------------
WITH session_stages AS (
    SELECT
        f.session_id,
        p.category_id,
        p.category                         AS category,
        d.device_name                      AS device,
        l.city                             AS city,
        date_trunc('hour', t.minute_ts)    AS hour,
        MAX(CASE WHEN f.stage = 'page_view'   THEN 1 ELSE 0 END) AS viewed,
        MAX(CASE WHEN f.stage = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
        MAX(CASE WHEN f.stage = 'purchase'    THEN 1 ELSE 0 END) AS bought
    FROM tiendacol_gold.fact_funnel_event f
    JOIN tiendacol_gold.dim_product  p ON p.product_key  = f.product_key
    JOIN tiendacol_gold.dim_device   d ON d.device_key   = f.device_key
    JOIN tiendacol_gold.dim_location l ON l.location_key = f.location_key
    JOIN tiendacol_gold.dim_datetime t ON t.datetime_key = f.datetime_key
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    category_id,
    category,
    device,
    city,
    hour,
    SUM(viewed) AS page_view,
    SUM(carted) AS add_to_cart,
    SUM(bought) AS purchase,
    TRY(1.0 * SUM(carted) / NULLIF(SUM(viewed), 0)) AS view_to_cart,
    TRY(1.0 * SUM(bought) / NULLIF(SUM(carted), 0)) AS cart_to_purchase,
    TRY(1.0 * SUM(bought) / NULLIF(SUM(viewed), 0)) AS view_to_purchase
FROM session_stages
GROUP BY 1, 2, 3, 4, 5
ORDER BY hour DESC, category_id, city, device;


-- -----------------------------------------------------------------------------
-- 2. RF-11 · VENTAS NETAS, ÓRDENES ÚNICAS Y TICKET PROMEDIO
-- Regla de negocio crítica: Ticket promedio = SUM(net_value) / COUNT(DISTINCT order_id)
-- (nunca promedio de promedios, porque una orden puede tener varias líneas).
-- -----------------------------------------------------------------------------
SELECT
    t.date                                                      AS date,
    p.category_id                                               AS category_id,
    p.category                                                  AS category,
    s.seller_id                                                 AS seller_id,
    s.seller_name                                               AS seller_name,
    l.city                                                      AS city,
    l.department                                                AS region,
    SUM(f.net_value)                                            AS sales_cop,
    COUNT(DISTINCT f.order_id)                                  AS orders,
    SUM(f.net_value) / NULLIF(COUNT(DISTINCT f.order_id), 0)    AS avg_ticket
FROM tiendacol_gold.fact_sales f
JOIN tiendacol_gold.dim_product  p ON p.product_key  = f.product_key
JOIN tiendacol_gold.dim_seller   s ON s.seller_key   = f.seller_key
JOIN tiendacol_gold.dim_location l ON l.location_key = f.location_key
JOIN tiendacol_gold.dim_datetime t ON t.datetime_key = f.datetime_key
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY date DESC, sales_cop DESC;


-- -----------------------------------------------------------------------------
-- 3. AUDITORÍA SCD TIPO 2 EN dim_product
-- Muestra los productos que han cambiado de precio o categoría a lo largo del
-- tiempo, comparando sus versiones históricas (is_current = false) con la actual.
-- -----------------------------------------------------------------------------
SELECT
    product_id,
    product_key,
    name,
    category_id,
    category,
    price,
    valid_from,
    valid_to,
    is_current
FROM tiendacol_gold.dim_product
WHERE product_id IN (
    SELECT product_id
    FROM tiendacol_gold.dim_product
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
ORDER BY product_id, valid_from;


-- -----------------------------------------------------------------------------
-- 4. RF-13 (OPCIONAL) · PRODUCTOS COMPRADOS JUNTOS (AFINIDAD DE CESTA)
-- Devuelve para cada producto el Top-5 de productos que más se compran en la
-- misma orden.
-- -----------------------------------------------------------------------------
SELECT
    a.product_id,
    p1.name         AS product_name,
    a.related_product_id,
    p2.name         AS related_product_name,
    a.co_orders,
    a.rank
FROM tiendacol_gold.agg_product_affinity a
LEFT JOIN tiendacol_gold.dim_product p1
    ON p1.product_id = a.product_id AND p1.is_current = true
LEFT JOIN tiendacol_gold.dim_product p2
    ON p2.product_id = a.related_product_id AND p2.is_current = true
WHERE a.rank <= 5
ORDER BY a.product_id, a.rank;


-- -----------------------------------------------------------------------------
-- 5. RNF-06 · AUDITORÍA DE TRAZABILIDAD GOLD -> BRONZE
-- Comprueba que el 100% de los hechos en Gold tienen su event_id en Bronze
-- (resultado esperado: unlinked_events = 0).
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                                AS total_gold_rows,
    COUNT(b.event_id)                                       AS linked_to_bronze,
    COUNT(*) - COUNT(b.event_id)                            AS unlinked_events
FROM (
    SELECT event_id FROM tiendacol_gold.fact_sales
    UNION ALL
    SELECT event_id FROM tiendacol_gold.fact_funnel_event
) g
LEFT JOIN tiendacol_bronze.events b
    ON b.event_id = g.event_id;


-- -----------------------------------------------------------------------------
-- 6. FRESCURA ANALÍTICA (SLA <= 60 MINUTOS PARA RF-10 Y RF-11)
-- Calcula los minutos transcurridos desde el último hecho registrado en Gold.
-- -----------------------------------------------------------------------------
SELECT
    MAX(t.minute_ts)                                            AS last_fact_timestamp,
    date_diff('minute', MAX(t.minute_ts), current_timestamp)    AS minutes_since_last_fact
FROM tiendacol_gold.fact_sales f
JOIN tiendacol_gold.dim_datetime t ON t.datetime_key = f.datetime_key;
