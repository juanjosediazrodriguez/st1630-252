# P2 · Streaming de TiendaCol (Kinesis → AWS Glue Streaming → Delta Lake)

Implementa la parte P2 de "TiendaCol en tiempo real" (ST1630 Sistemas Intensivos en Datos, EAFIT).
El contrato (tablas, reglas, mensajes) está en [`DISENO.md`](DISENO.md); este archivo explica cómo correrlo.

## Estructura

```
DISENO.md                         contrato de P2 (qué lee, qué escribe, con qué columnas)
glue/tiendacol_streaming.py       punto de entrada del job de Glue (parse_known_args)
tiendacol_stream/                 paquete con la lógica (va como --extra-py-files en un .zip)
  config.py    argumentos del job y SparkSession       parse.py     validación y cuarentena (funciones puras)
  sources.py   Kinesis / archivos / Bronze              enrich.py    enriquecimiento con maestros (broadcast join)
  masters.py   carga y recarga de maestros              ingesta.py   Q1  Bronze + cuarentena + Silver (MERGE)
  tables.py    esquemas y creación de tablas Delta      stock.py     Q2  stock actual (RF-03)
  deltaio.py   append idempotente, MERGE, triggers      alertas.py   Q3  agotamiento (RF-07)
  sqs.py       publicación en SQS (aws/file/off)        carritos.py  Q4  carrito abandonado (RF-08)
  job.py       orquestación                             ventas_minuto.py  Q5  ventas por minuto (RF-09)
tests/                            pytest: unitarias + integración con los escenarios de P1
scripts/                          run_tests.sh, run_local.sh (dentro del contenedor) y docker_run.sh (desde el host)
sql/athena_ddl.sql                tablas Delta en Athena
infra/glue_job.md                 definición del job, parámetros e IAM (para P4)
```

Cada consulta tiene su lógica en funciones puras `DataFrame -> DataFrame` (`aggregate_stock`, `current_windows`,
`evaluate_alerts`, `abandoned_sessions`, `minute_agg`, ...) y un envoltorio delgado `start_<consulta>(spark, cfg)`
que devuelve el `StreamingQuery`. Así la lógica se prueba sin streaming.

## Cómo correr en local

Hace falta Docker y la imagen de Glue 4.0 (Spark 3.3, Python 3.10, Delta 2.1):

```bash
docker pull public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01
```

**Pruebas** (desde Git Bash; `docker_run.sh` monta `entregable2/` en el contenedor y activa Delta):

```bash
cd proyecto-final/entregable2/p2-streaming
scripts/docker_run.sh "scripts/run_tests.sh"                    # todo
scripts/docker_run.sh "scripts/run_tests.sh tests/test_parse.py -v"
```

La imagen necesita `-e DATALAKE_FORMATS=delta` (carga `delta-core 2.1.0`) y `-e TZ=UTC`; `docker_run.sh` ya los pone.
Las pruebas crean el lago en una carpeta temporal del contenedor (`/tmp`), sin credenciales y sin red.

**Job completo con archivos en vez de Kinesis** (dentro del contenedor):

```bash
# 1) maestros y eventos de P1 en una carpeta de trabajo
python -m tiendacol_gen masters ...            # ver p1-generador/README.md; salida en <trabajo>/lake/landing/masters
python -m tiendacol_gen run --sink file ...    # un JSONL por stream (clickstream.jsonl, orders.jsonl, stock_changes.jsonl)
# 2) empaquetar y correr (procesa lo pendiente en varios micro-batches y termina)
(cd /home/glue_user/workspace/entregable2/p2-streaming && zip -qr tiendacol_stream.zip tiendacol_stream)
scripts/run_local.sh <trabajo> <carpeta_con_jsonl> --queries ingesta,stock,ventas_minuto
```

Para ver el watermark avanzar, parta la entrada en varios archivos (`clickstream_0001.jsonl`, `clickstream_0002.jsonl`...)
y use `--max_files_per_trigger 1`. El nombre del archivo (sin el sufijo `_NNNN`) es el `stream` lógico. Las decisiones
(alertas, carritos) salen a `<trabajo>/decisions/alerts.jsonl` y `carts.jsonl`, con `received_at`.

## Argumentos del job

Son los mismos en Glue y en local. Los que más se usan:

| Argumento | Defecto | Descripción |
|---|---|---|
| `--lake_root`, `--checkpoint_root` | (obligatorios) | raíz del lago y de los checkpoints (`s3://...` o carpeta) |
| `--masters_root` | `<lake_root>/landing/masters` | maestros de P1 |
| `--source` | `kinesis` | `kinesis`, `files` (JSONL, con `--input_path`) o `bronze` (reproceso) |
| `--stream_name`, `--region`, `--kinesis_endpoint`, `--starting_position`, `--kinesis_format` | `tiendacol-events`, `us-east-1`, regional, `TRIM_HORIZON`, `kinesis` | fuente Kinesis |
| `--sqs_mode` | `aws` | `aws`, `file` (JSONL en `--decisions_dir`) u `off`; con `aws`: `--alerts_queue_url`, `--carts_queue_url` |
| `--ingest_trigger_s`, `--downstream_trigger_s` | 30, 10 | cadencia de Q1 y de Q2–Q5 |
| `--run_once` | falso | `availableNow`: procesa lo pendiente y termina (Q1 y luego Q2–Q5 una por una) |
| `--queries` | las cinco | `ingesta,stock,alertas,carritos,ventas_minuto` |
| `--shuffle_partitions` | 8 | particiones de shuffle y de estado (el 200 por defecto hace lenta cada operación) |
| `--delta_max_files_per_trigger`, `--max_files_per_trigger` | sin límite | tamaño de micro-batch al leer Silver / archivos (pruebas) |
| umbrales | 60 min, 15 min, 60 min, COP 500.000, 30 min, `1 minute`/`3 minutes` | `--alert_horizon_min`, `--alert_window_min`, `--alert_cooldown_min`, `--cart_min_value`, `--cart_gap_min`, `--alert_watermark`/`--sales_watermark`, `--cart_watermark` |

## Despliegue en Glue (para P4)

Ver [`infra/glue_job.md`](infra/glue_job.md): empaquetado en `.zip`, Glue 4.0 `gluestreaming`, G.1X × 2, `--datalake-formats delta`,
`--conf` con las extensiones de Delta, todos los argumentos y los permisos IAM (Kinesis, S3, SQS, logs).
Las tablas para Athena están en [`sql/athena_ddl.sql`](sql/athena_ddl.sql).

## Contratos de las colas SQS

Referencia: `DISENO.md` §4.1 (alertas) y §4.2 (carritos). Resumen para el consumidor (Lambda de P4):

| Cola | `decision_type` | Clave de idempotencia | Notas |
|---|---|---|---|
| `tiendacol-alertas` | `stock_depletion_alert` | `alert_id` = `<product_id>\|<window_end>` | un reintento del lote produce el mismo id |
| `tiendacol-carritos` | `abandoned_cart` | `user_id` (upsert en DynamoDB); `message_id` = `<user_id>\|<abandoned_at>` | `abandoned_date` en hora de Colombia, para el GSI de fecha y valor |

La entrega es *at-least-once*: el consumidor debe descartar repetidos. Todos los mensajes llevan `schema_version: "1.0"`.

## Garantías y decisiones de diseño

- **Una sola lectura de Kinesis.** Solo Q1 lee la fuente; Q2–Q5 leen Silver. Evita *throttling* (5 `GetRecords`/s por shard)
  y hace que las decisiones usen datos ya deduplicados.
- **Idempotencia.** Bronze y cuarentena: append con `txnAppId`/`txnVersion = batch_id` (Delta descarta el segundo intento del
  mismo lote). Silver: `MERGE` de solo inserción por `(event_date, event_id)`. Stock, ventas por minuto y `alert_log`:
  `MERGE` con `SET` desde el estado, no con incrementos. Alertas: `alert_id` determinista.
  `txnAppId` incluye el id de la consulta (de `<checkpoint>/metadata`): si se borra el checkpoint, el contador de lotes
  reinicia en 0 sin chocar con versiones recordadas por Delta.
- **Latencia.** Presupuesto de `DISENO.md` §1 (≈ 75 s en el peor caso). Medidas que lo hacen viable: pocas particiones de
  shuffle, `spark.databricks.delta.snapshotPartitions` igual a las de shuffle (50 por defecto = 50 tareas por commit),
  `persist` del lote y de la validación para no releer la fuente ni recalcular el JSON, y `localCheckpoint` antes del
  `MERGE`. Cada micro-batch de Q1 registra los segundos de cada paso (`stats["seconds"]`).
- **Archivos pequeños.** `--write_files` (1 por defecto) hace `coalesce` antes de escribir cada micro-batch.
- **Poda de particiones.** `MERGE` y lecturas filtran por `event_date`/`ingest_date`.

## Evidencia: fuente de Kinesis en Glue 4.0

Revisión de la imagen `aws-glue-libs:glue_libs_4.0.0_image_01` (la misma versión de bibliotecas que el runtime de Glue 4.0):

- Jar: `/home/glue_user/spark/jars/spark-sql-kinesis_2.13-1.2.1_spark-3.3.jar` (conector tipo Qubole, 43 MB).
  Otros jars relacionados: `spark-streaming-kinesis-asl_2.12-3.3.0.jar` (DStreams, no sirve para Structured Streaming) y
  `aws-java-sdk-kinesis-1.12.528.jar` (`/home/glue_user/aws-glue-libs/jars`).
- `META-INF/services/org.apache.spark.sql.sources.DataSourceRegister` → `org.apache.spark.sql.kinesis.KinesisSourceProvider`;
  su `shortName` es **`kinesis`** (`spark.readStream.format("kinesis")`).
- Opciones leídas por el proveedor (en minúsculas): `streamname`, `endpointurl`, `regionname`, `startingposition`
  (`trim_horizon`, `latest`, `earliest`, `at_timestamp`), `failondataloss`, `awsaccesskeyid`, `awssecretkey`,
  `awsuseinstanceprofile`, `awsstsrolearn`, `sessiontoken`, `client.describeshardinterval`, `kinesis.executor.*`.
- Esquema fijo (comprobado con `spark.readStream.format("kinesis")...load().schema` en la imagen):
  `data:binary, streamName:string, partitionKey:string, sequenceNumber:string, approximateArrivalTimestamp:timestamp`.
  **No expone el shard**: `bronze.shard_id` queda nulo (previsto en `DISENO.md` §3.1).
- `sources.py` usa ese formato y esas opciones; el nombre es configurable con `--kinesis_format`.

**Pendiente prueba de humo en AWS:** no se pudo leer un stream real (sin red ni credenciales). Faltan por confirmar en el
Learner Lab: que `CreateStream` esté permitido, que el rol pueda leerlo y que el conector acepte `endpointUrl` regional.

## Otras verificaciones hechas en la imagen (Spark 3.3 + Delta 2.1)

- `txnAppId`/`txnVersion` en `DataFrameWriter`: funcionan (tres appends con la misma versión dejan una sola escritura).
- `from_json` a `map<string,string>`: da `null` para JSON inválido o truncado y deja los demás campos como texto para
  castear después (detecta `wrong_type` sin perder el registro). Se prefirió a `columnNameOfCorruptRecord` porque un esquema
  tipado anula la fila entera ante un solo campo erróneo y no se sabría cuál falló.
- El módulo Python `delta` **no** está disponible en la imagen: los `MERGE` se hacen con SQL (`MERGE INTO delta.\`ruta\``),
  que requiere las extensiones de Delta y **no** el catálogo de Glue.
- `session_window` con `collect_set` en modo *append* funciona en Structured Streaming 3.3.

## Desviaciones respecto a `DISENO.md`

1. **Q3 calcula el stock a la hora de la última venta, no con el estado actual de `stock_current`** (§4.1). `stock` =
   `base_stock` (sumado en `stock_current`) + Σ `delta` hasta `last_sale_ts` (leído de Silver, solo los productos del
   lote). Motivo: Q2 y Q3 corren en paralelo; con el estado actual, el resultado depende de cuánto se haya adelantado
   Q2 (en `--run_once` Q2 termina antes y la alerta del RF-07 saldría en la venta #62 en vez de la #65). En tiempo real
   ambos valores coinciden.
2. **`MERGE` por SQL** (`MERGE INTO delta.\`ruta\``) en lugar de la API Python de `DeltaTable`: la imagen de Glue 4.0 no
   trae el módulo `delta`. Requiere las extensiones de Delta en la sesión (`--conf` en Glue) y no el catálogo de Glue.
3. **Q4 descarta la clave `__watermark__` con `events_count > 0`**, no con `user_key != '__watermark__'` (§4.2). Un
   filtro sobre la clave de agrupación se empuja por debajo del agregado y de `withWatermark`; se comprobó que entonces
   el watermark solo avanzaba con los eventos de carrito y la sesión nunca cerraba. `events_count` es un agregado y no
   se puede empujar.
4. **El enfriamiento de Q3 se mide en tiempo de evento** (`last_sale_ts`), no con la hora de reloj de `detected_at`,
   para que el reproceso de un día se comporte igual que el tiempo real.
5. **`ingest_date` y `event_date` usan la hora de Colombia** (UTC-5); los timestamps siguen en UTC.
6. **Archivos de entrada (`--source files`)**: `sequence_number` es archivo + huella del contenido (Spark no permite
   `monotonically_increasing_id` en streaming); es solo linaje de pruebas.
7. **`message_id` del carrito** es `<user_id>|<session_start>` (decisión tomada en `carritos.py`; `DISENO.md` §4.2 no
   fija cuál de las marcas usa). El consumidor debe tratarlo como opaco y hacer *upsert* por `user_id`.

### Inconsistencia detectada en P1 (no se modificó)

`line_value = quantity × unit_price × (1 − discount)` (DISENO §3.3) trata `discount` como fracción, pero
`EventFactory.order_created` calcula `total = Σ(quantity × unit_price − discount)`, es decir, como monto. Hoy todos los
escenarios usan `discount = 0.0` y no hay efecto; si P1 empezara a generar descuentos, `Σ line_value` y `total` dejarían
de coincidir. Hay que acordar una de las dos semánticas.
