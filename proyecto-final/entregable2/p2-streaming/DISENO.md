# P2 · Streaming de TiendaCol: Kinesis → AWS Glue Streaming

Responsable: Sebastián Durán (P2). RF cubiertos: RF-01, RF-02, RF-03, RF-05, RF-06, RF-07, RF-08, RF-09 y RNF-03.

Este documento es el contrato de la parte P2: qué lee, qué escribe, con qué columnas y por qué.
Los valores compartidos del E1 se usan tal cual: trigger de 30 s, watermark de 1 min (RF-07, RF-09)
y de 3 min (RF-08), alerta ≤ 2 min con horizonte de 60 min y velocidad de los últimos 15 min,
carrito ≥ COP 500.000 con 30 min de inactividad publicado ≤ 5 min después, ventas por minuto ≤ 2 min,
deduplicación por `event_id` con entrega *at-least-once* y `MERGE` idempotente en Delta Lake.

## 1. Topología: un job de Glue Streaming, cinco consultas

Un solo job de Glue 4.0 (Spark 3.3, Delta 2.1) corre cinco consultas de Structured Streaming en la
misma aplicación de Spark. Solo la primera lee Kinesis; las demás leen Silver como fuente Delta.

| Consulta | Lee | Escribe (único escritor) | Trigger | RF |
|---|---|---|---|---|
| Q1 `ingesta` | Kinesis `tiendacol-events` (o archivos en local) | `bronze/events` (append), `quarantine/events` (append), `silver/events` (`MERGE` solo inserción) | 30 s | RF-01, 02, 05, 06, RNF-03 |
| Q2 `stock` | `silver/events` (`stock_change`) | `silver/stock_current` (`MERGE`) | 10 s | RF-03 |
| Q3 `alertas` | `silver/events` (ventas) + `silver/stock_current` | SQS `tiendacol-alertas`, `silver/alert_log` | 10 s | RF-07 |
| Q4 `carritos` | `silver/events` (clickstream) | SQS `tiendacol-carritos` | 10 s | RF-08 |
| Q5 `ventas_minuto` | `silver/events` (`order_created`) | `gold/agg_sales_minute` (`MERGE`) | 10 s | RF-09 |

Por qué así:

- **Una sola lectura de Kinesis.** Un shard admite 5 llamadas `GetRecords` por segundo y 2 MB/s
  compartidos entre consumidores. Cinco consultas leyendo Kinesis se estorbarían (*throttling*).
- **Lo derivado sale de Silver, no del crudo.** Si la alerta del RF-07 contara desde Kinesis, el 1 %
  de duplicados inflaría la velocidad de venta. Silver ya está validado, deduplicado y enriquecido.
- **Silver sirve de fuente de streaming** porque el `MERGE` es solo de inserción: Delta lo registra
  como archivos nuevos, sin reescrituras, y la fuente Delta lo lee como si fuera *append*.
- **Presupuesto de latencia** (peor caso): espera del trigger de Q1 (30 s) + proceso de Q1 (~20 s)
  + trigger de Q2–Q5 (10 s) + proceso (~10 s) + SQS y Lambda (~5 s) ≈ 75 s < 120 s. El trigger
  compartido de 30 s se mantiene en la ingesta; los 10 s de las consultas derivadas solo existen
  para que la cadena quepa en los 2 minutos.
- **Un escritor por tabla.** Ninguna tabla tiene dos escritores. Glue Batch (P3) escribe todo Gold
  menos `agg_sales_minute`, que necesita latencia de minutos y por eso la escribe Q5.

## 2. Rutas

`<lake>` es el bucket del lago (`s3://...` en AWS o una carpeta local en pruebas).

| Ruta | Tabla | Partición |
|---|---|---|
| `<lake>/landing/masters/` | maestros de P1 (`<entidad>/dt=AAAA-MM-DD/`, `_manifest_dt=<día>.json`) | `dt` |
| `<lake>/bronze/events/` | eventos crudos | `ingest_date` |
| `<lake>/quarantine/events/` | rechazados con su causa | `ingest_date` |
| `<lake>/silver/events/` | eventos válidos, únicos y enriquecidos | `event_date` |
| `<lake>/silver/stock_current/` | stock por producto y bodega | — |
| `<lake>/silver/alert_log/` | alertas de agotamiento emitidas | — |
| `<lake>/gold/agg_sales_minute/` | ventas por minuto y categoría | — |
| `<checkpoints>/<consulta>/` | checkpoints de cada consulta (bucket de ops) | — |

## 3. Contratos de tablas

### 3.1 `bronze/events` — append, todo lo que llega

| Columna | Tipo | Nota |
|---|---|---|
| `raw` | string | payload tal cual llegó (UTF-8) |
| `event_id`, `event_type` | string | extraídos con `get_json_object`; pueden ser nulos |
| `stream` | string | nombre del stream de Kinesis (o del archivo en local) |
| `partition_key`, `sequence_number`, `shard_id` | string | linaje de Kinesis; `shard_id` es nulo si el conector no lo expone |
| `arrival_ts` | timestamp | `approximateArrivalTimestamp` de Kinesis |
| `ingested_at` | timestamp | hora del micro-batch |
| `batch_id` | bigint | id del micro-batch de Q1 |
| `ingest_date` | date | partición |

Conserva duplicados y malformados (RNF-03: 10.100 en Bronze para 10.000 únicos). El append es
idempotente frente a reintentos del micro-batch con las opciones `txnAppId`/`txnVersion=batch_id`
de Delta: si Glue reintenta el lote, Delta descarta la segunda escritura.

### 3.2 `quarantine/events` — append, una fila por registro rechazado

`raw`, `event_id`, `event_type`, `stream`, `sequence_number`, `reason`, `ingested_at`, `batch_id`, `ingest_date`.

`reason` toma la primera regla que falla, en este orden:

1. `invalid_json` — el payload no es JSON (P1: `truncated_json`).
2. `missing_field:<campo>` — falta `event_id`, `event_type` o `event_ts`.
3. `unknown_event_type` — `event_type` fuera de `page_view, search, add_to_cart, remove_from_cart,
   checkout_start, purchase, order_created, order_status_changed, stock_change` (P1: `teleport`).
4. `wrong_type:<campo>` — `event_ts` no es ISO-8601 (P1: `"ayer en la tarde"`) o un campo numérico
   no se puede convertir.
5. `missing_field:<campo>` por tipo: clickstream exige `session_id`; `add_to_cart` exige
   `product_id`, `quantity`, `unit_price`; `remove_from_cart` exige `product_id`, `quantity`;
   `purchase` exige `order_id`; `order_created` exige `order_id`, `total` y `lines` no vacío;
   `order_status_changed` exige `order_id`, `status`; `stock_change` exige `product_id`,
   `warehouse_id`, `delta`, `reason`.
6. `unsupported_schema_version` — `schema_version` distinto de `"1.0"` (si falta, se asume `"1.0"`).

Las cuatro malformaciones de P1 caen en estas reglas (RF-05: 100 en cuarentena).

### 3.3 `silver/events` — una fila por `event_id`

| Grupo | Columnas |
|---|---|
| Envoltorio | `event_id`, `event_type`, `event_ts` (timestamp UTC), `event_date` (date en hora de Colombia), `schema_version`, `stream` (`clickstream`/`orders`/`stock_changes`, según el tipo) |
| Clickstream | `session_id`, `user_id`, `is_identified` (bool), `device`, `product_id`, `query`, `quantity` (int), `unit_price` (double), `cart_value` (double) |
| Órdenes | `order_id`, `status`, `previous_status`, `total` (double), `payment_method`, `payment_amount` (double), `payment_status`, `lines` (array de struct `product_id`, `quantity` int, `unit_price` double, `discount` double, `category_id` int, `seller_id`, `line_value` double) |
| Stock | `warehouse_id`, `delta` (int), `reason` |
| Enriquecimiento (RF-06) | `city`, `department`, `geo_source` (`customer_master` o `event`), `category_id` (int), `category`, `seller_id`, `product_name`, `list_price` (double), `masters_dt` |
| Linaje | `arrival_ts`, `ingested_at`, `sequence_number`, `batch_id`, `is_late` (bool: llegó más de 60 s después de su `event_ts`) |

Reglas:

- **Enriquecimiento (RF-06).** Se hace un *broadcast join* con los maestros vigentes. Si el usuario
  está identificado y existe en `customers`, `city`/`department` salen del maestro; si no, del
  evento. `category_id`, `category`, `seller_id`, `product_name` y `list_price` salen de `products`
  (también para cada línea de `lines`). `line_value = quantity × unit_price × (1 − discount)`.
- **Deduplicación (RF-05, RNF-03).** Primero `dropDuplicates(event_id)` dentro del lote; luego
  `MERGE ... ON t.event_date = s.event_date AND t.event_id = s.event_id WHEN NOT MATCHED INSERT`.
  Un duplicado es byte a byte igual al original, así que tiene el mismo `event_ts` y la misma
  `event_date`: la condición con fecha es exacta contra **toda** la tabla y, además, poda
  particiones. Reintentar el lote no cambia el resultado (idempotencia).
- **Tardíos.** Un evento tardío (2–3 min en las pruebas de P1) entra a Silver: Q1 no usa watermark.
  Las ventanas de Q3–Q5 sí lo descartan, como asumió el E1; Gold batch (P3) lo recupera al
  recalcular la hora.

### 3.4 `silver/stock_current` — stock como estado (RF-03)

`product_id`, `warehouse_id`, `base_stock` (int, inventario del maestro), `cumulative_delta`
(bigint), `stock` (bigint = base + acumulado), `masters_dt`, `last_event_ts`, `updated_at`.

Q2 agrupa `stock_change` por `(product_id, warehouse_id)` con `sum(delta)` en modo *update*. El
acumulado vive en el *state store* de Spark, respaldado por el checkpoint en S3. El estado no
necesita watermark porque está acotado por el catálogo (productos × bodegas). El `foreachBatch`
hace `MERGE` con `SET` (no con incrementos), así que reintentar el lote es idempotente. Al
arrancar, el job siembra la tabla con el inventario del maestro (inserta los que faltan con
acumulado 0). `base_stock` se escribe una sola vez y no se re-basa con maestros posteriores
(limitación conocida: en producción habría un corte de conciliación diario).

### 3.5 `gold/agg_sales_minute` — ventas por minuto (RF-09)

`minute_start` (timestamp UTC), `minute_end`, `category_id` (int; **NULL = fila total**), `category`
(`Total` en la fila total), `orders` (bigint), `sales_cop` (double), `updated_at`.

Q5 lee `order_created` con watermark de 1 min. Por cada orden emite una fila total y una fila por
cada categoría distinta de sus líneas (`array_distinct`), con el valor de esas líneas. Después
agrupa por `window(event_ts, '1 minute')` y `category_id` con `count(*)` y `sum`, en modo *update*:
cada micro-batch actualiza el minuto en curso, sin esperar a que cierre. El `foreachBatch` hace
`MERGE ON t.minute_start = s.minute_start AND t.category_id <=> s.category_id` (`<=>` es la
igualdad que trata NULL = NULL, necesaria para la fila total). Las filas de categoría suman
exactamente la fila total porque ambas usan `line_value`.

### 3.6 `silver/alert_log` — alertas emitidas (RF-07)

`alert_id`, `product_id`, `product_name`, `category_id`, `stock`, `units_15m`,
`velocity_per_min`, `coverage_min`, `window_start`, `window_end`, `last_sale_ts`, `detected_at`.

## 4. Decisiones en tiempo real

### 4.1 RF-07 — alerta de agotamiento (Q3)

- Ventas = `stock_change` con `reason = 'sale'`; unidades = `abs(delta)`.
- `withWatermark(event_ts, '1 minute')`, agrupado por `product_id` y `window(event_ts, '15 minutes',
  '1 minute')` con `sum(units)`, en modo *update*.
- En cada lote, la ventana vigente de un producto es la que termina en el primer minuto exacto
  después de su última venta: `window.end = date_trunc('minute', max_sale_ts) + 1 min`. Esa
  ventana cubre los últimos 15 minutos y se recalcula cada minuto, como pide el E1.
- `velocidad = unidades_ventana / 15` (siempre entre 15, también antes de que la ventana se llene,
  igual que el oráculo de P1); `stock` = suma de `silver/stock_current` del producto;
  `cobertura = stock / velocidad`. Hay alerta si `velocidad > 0` y `cobertura < 60`.
- Enfriamiento: no se repite la alerta del mismo producto si hay otra en `alert_log` de los últimos
  60 min. `alert_id = <product_id>|<window_end ISO>` es determinista: un reintento del lote produce
  el mismo id y la Lambda de P4 lo descarta.
- Orden: primero se publica en SQS y después se escribe `alert_log`. Si el job cae entre ambos
  pasos, el reintento publica otra vez el mismo `alert_id` (*at-least-once* + consumidor
  idempotente). Al revés se podría perder la alerta.

Mensaje a `tiendacol-alertas`:

```json
{"schema_version": "1.0", "decision_type": "stock_depletion_alert", "alert_id": "TEST-RF07|2026-11-09T15:14:00.000Z",
 "product_id": "TEST-RF07", "product_name": "...", "category_id": 1, "stock": 255, "units_15m": 65,
 "velocity_per_min": 4.333, "coverage_min": 58.8, "window_start": "...", "window_end": "...",
 "last_sale_ts": "...", "detected_at": "..."}
```

### 4.2 RF-08 — carrito abandonado (Q4)

- Eventos de carrito: `add_to_cart` (+ cantidad × precio), `remove_from_cart` (− cantidad × precio;
  si el evento no trae precio se usa `list_price` del maestro), `checkout_start` (0) y `purchase`
  (marca de compra). Solo de usuarios identificados.
- `session_window(event_ts, '30 minutes')` por `user_id`, con watermark de 3 min, en modo *append*:
  la sesión se emite una sola vez, cuando el watermark pasa su fin (último evento de carrito + 30
  min). Las vistas de página no extienden la sesión ("30 min sin compra ni cambios").
- El watermark se calcula con **todos** los eventos de Silver, no solo con los de carrito. Para eso,
  los eventos que no son de carrito o de anónimos se agrupan bajo la clave `__watermark__` con valor
  0, que se descarta a la salida. Si se filtraran antes del watermark, Spark empujaría el filtro por
  debajo del operador y el latido del escenario de P1 (`page_view` cada 10 s) no movería el reloj.
- Se publica si `cart_value ≥ 500000` y no hubo `purchase`. Llega en ~33–34 min tras el último
  evento (30 de inactividad + 3 de watermark + la cadena): dentro de los 5 min que da el E1.

Mensaje a `tiendacol-carritos` (la Lambda de P4 hace *upsert* en DynamoDB por `user_id`):

```json
{"schema_version": "1.0", "decision_type": "abandoned_cart", "message_id": "U-TEST-A|2026-11-09T15:00:10.000Z",
 "user_id": "U-TEST-A", "cart_value": 600000.0, "currency": "COP", "session_start": "...",
 "last_event_ts": "...", "abandoned_at": "...", "abandoned_date": "2026-11-09", "events_count": 1,
 "product_ids": ["..."], "detected_at": "..."}
```

`abandoned_at = last_event_ts + 30 min`; `abandoned_date` va en hora de Colombia, para el GSI de
fecha y valor del E1.

### 4.3 Publicación en SQS

Los mensajes de decisión son pocos por lote. Se recogen en el driver y se envían con
`send_message_batch` (lotes de 10, 3 reintentos con espera). Si aun así fallan, el micro-batch
falla y Spark lo reintenta. Modos: `aws` (boto3), `file` (escribe JSONL con `received_at` para las
pruebas locales) y `off`.

## 5. Maestros

Al arrancar, el job busca el `_manifest_dt=*.json` más reciente bajo `landing/masters/` y carga
products, customers, sellers e inventory de ese día (CSV, o JSON si el manifiesto lo indica).
Usa el manifiesto porque P1 lo sube al final: si existe, la carga está completa. Cada 5 min, Q1
revisa si hay un manifiesto más nuevo y recarga los maestros. Si no hay ninguno, el job falla con
un error claro, porque sin maestros no se cumple el RF-06.

## 6. Reflexión E1 → E2

En el E1, Silver lo escribía solo el batch horario, por tres razones: un único escritor, la
deduplicación completa y la posibilidad de reprocesar desde Bronze. En el E2 Silver lo escribe Glue
Streaming y las tres se mantienen:

1. **Un único escritor:** solo Q1 escribe Silver; P3 lo lee.
2. **Deduplicación completa:** el `MERGE` por `(event_date, event_id)` es exacto contra toda la
   tabla (§3.3), no solo contra el lote o el watermark.
3. **Reproceso:** con `--source bronze`, el mismo job relee Bronze (append-only, sirve de fuente
   de streaming) y reconstruye Silver con el mismo código de validación.

Otros cambios: Kafka pasa a Kinesis (el *offset* pasa a ser `sequence_number`), Spark en EC2 pasa a
Glue (sin servidor que administrar; la EC2 queda solo para Airflow) y las decisiones salen por SQS
con una DLQ por caso de uso, en vez de escribirse directo en SNS y DynamoDB.

## 7. Pendientes que no dependen de P2

- P4: probar que el Learner Lab permite crear el stream de Kinesis (en el Lab 2b falló `CreateStream`).
- P1: el verificador del RF-09 espera la columna `value`; esta tabla usa `sales_cop`, como el SQL de
  Superset y la frase del E1 ("ventas en COP").
- Confirmar con el profesor que `agg_sales_minute` la escribe el streaming (corregir el diagrama:
  "Gold único escritor" → "Gold: batch, salvo `agg_sales_minute`").
- Prueba de humo en AWS: confirmar el nombre del conector de Kinesis en Glue 4.0 y sus columnas.
