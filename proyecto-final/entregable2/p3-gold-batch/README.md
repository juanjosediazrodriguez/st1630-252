# P3 · Capa Gold, Modelo Dimensional (SCD2) y Consultas en Athena

**Responsable:** Juan José Díaz Rodríguez (P3)  
**Requisitos:** Dimensiones **SCD Tipo 2**, **RF-10** (embudo de conversión), **RF-11** (ventas y ticket promedio), **RF-13** (afinidad de productos) y **RNF-06** (trazabilidad `Gold → Bronze`).

---

## Estructura del Directorio

| Ruta | Descripción |
| :--- | :--- |
| [`DISENO.md`](DISENO.md) | Contrato técnico del esquema en estrella, grano de tablas y reglas SCD Tipo 2. |
| `glue/tiendacol_gold_batch.py` | Punto de entrada del Job de AWS Glue 4.0 (`glueetl`). |
| `tiendacol_gold/` | Paquete Python/PySpark con la lógica de dimensiones SCD2, tablas de hechos y consultas. |
| `sql/ddl_gold.sql` | Registro de tablas Delta nativas en AWS Glue Data Catalog / Amazon Athena v3. |
| `sql/consultas_athena.sql` | Consultas analíticas listas para Athena y Apache Superset (`RF-10`, `RF-11`, `RF-13`, `SCD2`, `RNF-06`). |
| `infra/glue_job.md` | Parámetros de despliegue del Glue Job y permisos IAM para P4 (CloudFormation y Airflow). |
| `tests/test_gold_p3.py` | Suite de pruebas automáticas contra los escenarios y el oráculo de P1 (`verify.py`). |
| [`BITACORA_IA_P3.md`](BITACORA_IA_P3.md) | Bitácora de delegación de IA de P3. |

---

## 1. Ejecución de Pruebas en Local (Sin AWS)

Las pruebas validan el SCD Tipo 2 multi-día, la idempotencia y la coincidencia exacta contra `funnel_oracle` (**RF-10**), `sales_oracle` (**RF-11**), `verify_rf13` (**RF-13**) y `verify_rnf06` (**RNF-06**) de P1:

```powershell
cd proyecto-final/entregable2/p3-gold-batch
python -m pytest -v
```

---

## 2. Ejecución del Job de Glue Batch

### En local (apuntando a una carpeta del lago generada por P1 y P2):

```powershell
python glue/tiendacol_gold_batch.py --lake_root ./out/lake
```

### En AWS Glue 4.0 (ver detalle completo en [`infra/glue_job.md`](infra/glue_job.md)):

```bash
# 1. Empaquetar y subir a S3 operativo
cd proyecto-final/entregable2/p3-gold-batch
zip -r tiendacol_gold.zip tiendacol_gold -x '*/__pycache__/*'
aws s3 cp tiendacol_gold.zip s3://<OPS_BUCKET>/glue/tiendacol_gold.zip
aws s3 cp glue/tiendacol_gold_batch.py s3://<OPS_BUCKET>/glue/tiendacol_gold_batch.py

# 2. Ejecutar bajo demanda o desde el DAG horario de Airflow
aws glue start-job-run --job-name tiendacol-gold-batch
```

---

## 3. Registro y Consulta en Amazon Athena

1. Abrir la consola de **Amazon Athena** en `us-east-1` y configurar el bucket de resultados (`s3://<OPS_BUCKET>/athena-results/`).
2. Ejecutar [`sql/ddl_gold.sql`](sql/ddl_gold.sql) reemplazando `<LAKE_BUCKET>` por el nombre del bucket del lago.
3. Ejecutar las consultas de negocio de [`sql/consultas_athena.sql`](sql/consultas_athena.sql) para visualizar:
   - Embudo de conversión por categoría, dispositivo, ciudad y hora (**RF-10**).
   - Ventas netas, órdenes únicas y ticket promedio por fecha, categoría, vendedor y región (**RF-11**).
   - Historial de versiones SCD Tipo 2 en `dim_product`.
   - Ranking de productos comprados juntos (**RF-13**).
   - Auditoría de trazabilidad `Gold → Bronze` (**RNF-06**).
