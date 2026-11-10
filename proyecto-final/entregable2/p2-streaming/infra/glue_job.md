# Definición sugerida del job de Glue Streaming (para P4)

Un solo job corre las cinco consultas (Q1..Q5) en la misma aplicación de Spark. Una sola lectura de Kinesis;
las consultas derivadas leen Silver (ver `DISENO.md` §1).

## Empaquetado

```bash
cd proyecto-final/entregable2/p2-streaming
zip -r tiendacol_stream.zip tiendacol_stream -x '*/__pycache__/*'
aws s3 cp tiendacol_stream.zip s3://<OPS_BUCKET>/glue/tiendacol_stream.zip
aws s3 cp glue/tiendacol_streaming.py s3://<OPS_BUCKET>/glue/tiendacol_streaming.py
```

## Job

| Propiedad | Valor |
|---|---|
| Nombre | `tiendacol-streaming` |
| Tipo | Streaming (`--command-name gluestreaming`) |
| Glue version | 4.0 (Spark 3.3, Python 3.10, Delta 2.1) |
| Worker | G.1X x 2 (el mínimo de un job de streaming; el driver ocupa uno) |
| Script | `s3://<OPS_BUCKET>/glue/tiendacol_streaming.py` |
| Reintentos | 1 o más (`MaxRetries`): el job es idempotente (ver `README.md`, "Garantías") |
| Timeout | sin límite práctico (streaming) |
| Rol | rol de ejecución con los permisos de la sección "IAM" |

### Parámetros del job (`DefaultArguments`)

```json
{
  "--job-language": "python",
  "--datalake-formats": "delta",
  "--extra-py-files": "s3://<OPS_BUCKET>/glue/tiendacol_stream.zip",
  "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
  "--enable-metrics": "true",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-spark-ui": "false",

  "--lake_root": "s3://<LAKE_BUCKET>",
  "--checkpoint_root": "s3://<OPS_BUCKET>/checkpoints/tiendacol-streaming",
  "--masters_root": "s3://<LAKE_BUCKET>/landing/masters",
  "--source": "kinesis",
  "--stream_name": "tiendacol-events",
  "--region": "us-east-1",
  "--starting_position": "TRIM_HORIZON",
  "--kinesis_format": "kinesis",
  "--sqs_mode": "aws",
  "--alerts_queue_url": "https://sqs.us-east-1.amazonaws.com/<ACCOUNT_ID>/tiendacol-alertas",
  "--carts_queue_url": "https://sqs.us-east-1.amazonaws.com/<ACCOUNT_ID>/tiendacol-carritos",
  "--ingest_trigger_s": "30",
  "--downstream_trigger_s": "10",
  "--shuffle_partitions": "8"
}
```

Notas sobre los parámetros:

- **No active `--enable-glue-datacatalog`.** El job escribe y lee tablas Delta por ruta, no por catálogo; activar el
  catálogo hace que cada `MERGE ... delta.\`ruta\`` consulte el metastore y añade latencia (en pruebas locales, sin
  región configurada, incluso falla).
- `--conf` lleva las extensiones de Delta de Spark SQL: son configuración *estática* y no se pueden fijar desde el
  código una vez que existe la sesión. El valor se escribe en una sola cadena con ` --conf ` intercalado (truco
  documentado de Glue para pasar varias).
- `--shuffle_partitions 8`: con el valor por defecto de Spark (200) cada operador con estado haría 200 tareas por
  micro-batch y la latencia se dispararía. Con 2 workers G.1X, de 4 a 8 es suficiente.
- Parámetros opcionales: `--kinesis_endpoint` (vacío = endpoint regional), `--delta_max_files_per_trigger`,
  `--queries` (por defecto las cinco), `--write_files`, y los umbrales (`--alert_horizon_min 60`,
  `--alert_window_min 15`, `--alert_cooldown_min 60`, `--cart_min_value 500000`, `--cart_gap_min 30`,
  `--alert_watermark "1 minute"`, `--sales_watermark "1 minute"`, `--cart_watermark "3 minutes"`).
  Glue agrega `--JOB_ID`, `--JOB_RUN_ID`, `--JOB_NAME`, ...: el script los ignora (`parse_known_args`).
- **Pendiente prueba de humo en AWS:** `--kinesis_format kinesis` es el conector que trae la imagen de Glue 4.0
  (`spark-sql-kinesis_2.13-1.2.1_spark-3.3.jar`). Si en el Learner Lab el formato tuviera otro nombre, se cambia solo
  con este parámetro.

### Ejemplo con la CLI

```bash
aws glue create-job --name tiendacol-streaming --role <ROL_EJECUCION> --glue-version 4.0 \
  --worker-type G.1X --number-of-workers 2 \
  --command Name=gluestreaming,ScriptLocation=s3://<OPS_BUCKET>/glue/tiendacol_streaming.py,PythonVersion=3 \
  --default-arguments file://default_arguments.json
aws glue start-job-run --job-name tiendacol-streaming
```

## Permisos IAM necesarios (rol de ejecución del job)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "KinesisLectura", "Effect": "Allow",
     "Action": ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards",
                "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:SubscribeToShard"],
     "Resource": "arn:aws:kinesis:us-east-1:<ACCOUNT_ID>:stream/tiendacol-events"},
    {"Sid": "S3Lago", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
     "Resource": ["arn:aws:s3:::<LAKE_BUCKET>", "arn:aws:s3:::<LAKE_BUCKET>/*"]},
    {"Sid": "S3OpsCheckpointsYCodigo", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
     "Resource": ["arn:aws:s3:::<OPS_BUCKET>", "arn:aws:s3:::<OPS_BUCKET>/*"]},
    {"Sid": "SqsDecisiones", "Effect": "Allow",
     "Action": ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
     "Resource": ["arn:aws:sqs:us-east-1:<ACCOUNT_ID>:tiendacol-alertas", "arn:aws:sqs:us-east-1:<ACCOUNT_ID>:tiendacol-carritos"]},
    {"Sid": "Logs", "Effect": "Allow",
     "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "cloudwatch:PutMetricData"],
     "Resource": "*"}
  ]
}
```

Además, el rol necesita la política administrada `AWSGlueServiceRole` (o equivalente) para que Glue pueda ejecutar el
job. En el Learner Lab se usa el `LabRole`, que ya incluye estos permisos; la lista anterior es la referencia del
mínimo necesario.

## Contratos de entrada y salida

- **Entrada:** stream de Kinesis `tiendacol-events`. Un shard admite 1 MB/s o 1.000 registros/s de escritura y 2 MB/s de lectura; para el pico de Black Friday hacen falta más shards (el código no cambia).
- **Maestros:** `s3://<LAKE_BUCKET>/landing/masters/` con `_manifest_dt=AAAA-MM-DD.json` (P1). Sin ningún manifiesto el
  job falla al arrancar con un mensaje claro (sin maestros no se cumple el RF-06).
- **Salida a SQS:** `tiendacol-alertas` y `tiendacol-carritos`, con el formato de `DISENO.md` §4. P4 debe crear una DLQ
  por cola y una Lambda idempotente: por `alert_id` (alertas) y por `user_id` (carritos, *upsert* en DynamoDB).
  Visibilidad sugerida: 30 s; retención: 1 h.
- **Tablas Delta:** ver `sql/athena_ddl.sql` (`table_type = 'DELTA'`).

## Operación

- **Reinicio:** relanzar el job con el mismo `--checkpoint_root` continúa donde quedó. Si se borra el checkpoint, hay
  que borrar también las tablas o reprocesar con `--source bronze` (Silver se reconstruye con el mismo código de
  validación; Bronze no se vuelve a escribir).
- **Reproceso:** `--source bronze --checkpoint_root s3://<OPS_BUCKET>/checkpoints/reproceso-<fecha> --queries ingesta`
  sobre una Silver vacía.
- **Archivos pequeños:** cada micro-batch escribe pocos archivos (`--write_files`, por defecto 1); P3 puede compactar
  con `OPTIMIZE` o con su batch horario.
