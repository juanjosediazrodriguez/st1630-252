# Definición del Job de AWS Glue Batch (P3 → para P4 / CloudFormation y Airflow)

El job `tiendacol-gold-batch` construye y actualiza de forma idempotente el Esquema en Estrella de la capa **Gold** leyendo `silver/events` (escrito por P2) y `landing/masters/` (escrito por P1).

---

## 1. Empaquetado y Subida a S3

```bash
cd proyecto-final/entregable2/p3-gold-batch
zip -r tiendacol_gold.zip tiendacol_gold -x '*/__pycache__/*'
aws s3 cp tiendacol_gold.zip s3://<OPS_BUCKET>/glue/tiendacol_gold.zip
aws s3 cp glue/tiendacol_gold_batch.py s3://<OPS_BUCKET>/glue/tiendacol_gold_batch.py
```

---

## 2. Propiedades del Job en AWS Glue

| Propiedad | Valor recomendado (cuidando presupuesto de USD 100) |
| :--- | :--- |
| **Nombre** | `tiendacol-gold-batch` |
| **Tipo** | Spark Batch (`--command-name glueetl`) |
| **Glue Version** | `4.0` (Spark 3.3, Python 3.10, Delta 2.1) |
| **Worker Type** | `G.1X` |
| **Number of Workers** | `2` (mínimo permitido; suficiente para el volumen de la demo) |
| **Timeout** | `15` minutos (evita cobros accidentales si un job queda colgado) |
| **MaxRetries** | `1` (el job usa `MERGE INTO` idempotente en todas las tablas) |
| **ScriptLocation** | `s3://<OPS_BUCKET>/glue/tiendacol_gold_batch.py` |

### `DefaultArguments` del Job

```json
{
  "--job-language": "python",
  "--datalake-formats": "delta",
  "--extra-py-files": "s3://<OPS_BUCKET>/glue/tiendacol_gold.zip",
  "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
  "--enable-metrics": "true",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-spark-ui": "false",
  "--lake_root": "s3://<LAKE_BUCKET>",
  "--masters_root": "s3://<LAKE_BUCKET>/landing/masters",
  "--shuffle_partitions": "8",
  "--write_files": "1",
  "--top_affinity_k": "10"
}
```

---

## 3. Integración con Airflow (DAG Horario de P4)

En el DAG horario (`dag_horario`), P4 invoca el job con `GlueJobOperator` (o `boto3.client("glue").start_job_run`):

```python
glue_client.start_job_run(
    JobName="tiendacol-gold-batch",
    Arguments={
        "--lake_root": f"s3://{LAKE_BUCKET}",
        "--process_date": "{{ ds }}",  # opcional: filtra Silver por fecha o procesa todo si se omite
    },
)
```

---

## 4. Permisos IAM Requeridos

El rol del job solo requiere lectura/escritura en S3 (`<LAKE_BUCKET>` y `<OPS_BUCKET>`) y publicación de logs en CloudWatch (cubiertos por el mismo rol de ejecución de Glue de P2).
