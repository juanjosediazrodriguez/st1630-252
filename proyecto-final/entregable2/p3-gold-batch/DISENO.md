# P3 · Capa Gold y Modelo Dimensional en AWS Glue Batch (SCD2 + Hechos + Athena)

**Responsable:** Juan José Díaz Rodríguez (P3)  
**Requisitos cubiertos:** SCD Tipo 2 en dimensiones, **RF-10** (embudo de conversión), **RF-11** (ventas netas, órdenes y ticket promedio), **RF-13** (afinidad de productos comprados juntos) y **RNF-06** (trazabilidad 100 % `Gold → Bronze`).

---

## 1. Arquitectura de la Capa Gold (Esquema en Estrella)

El job horario de **AWS Glue Batch (`tiendacol-gold-batch`)** es el único escritor de las dimensiones y tablas de hechos analíticas en `<lake_root>/gold/` (con la excepción de `agg_sales_minute`, que escribe Q5 de Glue Streaming para cumplir la latencia $\le 2\text{ min}$ del RF-09).

### 1.1 Puntas de la Estrella: Dimensiones (`dim_*`)

| Tabla | Ruta en S3 | Clave primaria | Tipo SCD | Descripción |
| :--- | :--- | :--- | :---: | :--- |
| `dim_product` | `<lake>/gold/dim_product/` | `product_key` | **SCD Tipo 2** | Conserva el historial de precios y cambios de categoría (`valid_from`, `valid_to`, `is_current`). |
| `dim_seller` | `<lake>/gold/dim_seller/` | `seller_key` | SCD Tipo 1 | Vendedores del marketplace (`seller_id`, `seller_name`, `city`, `department`). |
| `dim_customer` | `<lake>/gold/dim_customer/` | `customer_key` | SCD Tipo 1 | Clientes seudonimizados (`customer_id`, `city`, `department`, `created_at`). |
| `dim_location` | `<lake>/gold/dim_location/` | `location_key` | Conformada | Ubicación geográfica (`city`, `department`). |
| `dim_device` | `<lake>/gold/dim_device/` | `device_key` | Conformada | Canal / dispositivo (`desktop`, `mobile_web`, `app`). |
| `dim_datetime` | `<lake>/gold/dim_datetime/` | `datetime_key` | Conformada | Calendario a nivel de minuto (`minute_ts`, `date`, `hour`, `year`, `month`, `day`, `minute`). |

### 1.2 Centros de la Estrella: Tablas de Hechos (`fact_*`) y Agregados (`agg_*`)

| Tabla | Ruta en S3 | Grano | Partición | Requisito |
| :--- | :--- | :--- | :---: | :--- |
| `fact_funnel_event` | `<lake>/gold/fact_funnel_event/` | 1 fila por evento de etapa del embudo (`page_view`, `add_to_cart`, `purchase`) y producto | `event_date` | **RF-10, RNF-06** |
| `fact_sales` | `<lake>/gold/fact_sales/` | 1 fila por línea de producto dentro de una orden (`order_id + line_number`) | `event_date` | **RF-11, RNF-06** |
| `agg_product_affinity` | `<lake>/gold/agg_product_affinity/` | 1 fila por par `(product_id, related_product_id)` con conteo de órdenes compartidas y `rank` | — | **RF-13** |

---

## 2. Decisiones Técnicas Clave

1. **SCD Tipo 2 idempotente en `dim_product`:**
   - Compara cada snapshot diario de `landing/masters/products/dt=AAAA-MM-DD` contra la versión vigente (`is_current = true`).
   - Si cambia `price`, `category_id`, `category`, `seller_id` o `name`, cierra la versión previa (`is_current = false`, `valid_to = dt`) e inserta una nueva versión con llave subrogada determinista `surrogate_key("PROD", product_id, dt)`.
   - Si el mismo día se reprocesa, no duplica filas.
2. **Resolución Temporal (*Point-in-Time Lookup*):**
   - Al construir `fact_sales` y `fact_funnel_event`, cada evento se enlaza con la versión de `dim_product` cuya ventana `[valid_from, valid_to)` contenía la fecha del evento. Así un cambio de precio o categoría hoy no altera la contabilidad histórica de ayer.
3. **Resolución de `purchase` en el embudo (RF-10):**
   - En el generador (`EventFactory`), el evento `purchase` de `clickstream` lleva `order_id` pero `product_id = null`. El builder de P3 cruza `purchase` con las líneas de `order_created` (o el carrito de la sesión) para atribuir la compra al producto y categoría correspondientes, además de soportar `product_id` directo cuando viene poblado.
4. **Cálculo correcto del Ticket Promedio (RF-11):**
   - Como una orden tiene múltiples líneas en `fact_sales`, el ticket promedio se calcula siempre como `SUM(net_value) / COUNT(DISTINCT order_id)` y nunca como `AVG(net_value)` ni promedio de promedios.
5. **Compatibilidad nativa con AWS Glue 4.0 y Athena v3:**
   - Igual que en P2, los `MERGE` se ejecutan mediante SQL (`MERGE INTO delta.\`ruta\``) para no depender del paquete Python `delta`, y el DDL de Athena usa `TBLPROPERTIES ('table_type' = 'DELTA')` leyendo el esquema directamente desde `_delta_log`.
