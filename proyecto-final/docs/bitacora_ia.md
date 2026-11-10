# Bitácora consolidada de agentes de IA · TiendaCol E2

El equipo conserva el detalle por responsable en las bitácoras de
[P1](../entregable2/p1-generador/BITACORA_IA_P1.md),
[P2](../entregable2/p2-streaming/BITACORA_IA_P2.md),
[P3](../entregable2/p3-gold-batch/BITACORA_IA_P3.md) y
[P4](../entregable2/p4-infra-orquestacion/BITACORA_IA_P4.md).

| Frente | Agente usado | Trabajo delegado y revisión humana |
| --- | --- | --- |
| P1 · generador y verificación | Claude Code | Borradores de generador, maestros, escenarios y consultas de Superset. P1 revisó contratos y valores esperados; ejecutó 36 pruebas locales. |
| P2 · Glue Streaming | Claude Code (Opus y Sonnet) | Diseño y código de ingesta, Silver, alertas, carritos y ventas por minuto. P2 revisó umbrales, contratos y `MERGE`; ejecutó 56 pruebas y omitió 5 según su bitácora. |
| P3 · Gold | Gemini (Antigravity) | Asistencia en transformaciones, DDL y pruebas. P3 definió el modelo dimensional y verificó SCD2, RF-10/11/13 y trazabilidad con los oráculos de P1. |
| P4 · infraestructura y operación | Codex | Plantilla CloudFormation, Lambda, DAGs, despliegue, verificación AWS, monitoreo, documentación y correcciones de integración. Daniel y el equipo deben revisar y poder explicar decisiones y código. |

## Decisiones técnicas contrastadas con la implementación

- Se usa un stream Kinesis con 1 shard para la demostración y 6 para la prueba RNF-01. El generador usa `PutRecords`; Glue Streaming consume el stream.
- Se usa Delta Lake en S3 para Bronze, Silver y Gold. Glue Streaming escribe `agg_sales_minute` por el límite de latencia de RF-09; Glue Batch escribe el resto de Gold. Esto se aparta de la nota de un único escritor de Gold del diagrama E1 y se justifica por un escritor por tabla.
- Airflow en EC2 controla servicios administrados mediante sus API; el cálculo Spark permanece en Glue.
- SQS y Lambda ofrecen entrega al menos una vez. Las dos DLQ son independientes. El correo de alerta puede duplicarse si Lambda cae entre SNS Publish y el marcador de DynamoDB.
- S3 y Kinesis usan claves KMS administradas por AWS; SQS usa SSE administrado por el servicio. Los buckets S3 tienen retención al eliminar el stack.
- La cuenta AWS nueva requirió esperar el aprovisionamiento de claves KMS. El primer Gold Batch falló por valores `pandas.Timestamp` incompatibles con el esquema Spark; se normalizaron a `datetime` nativo y la prueba posterior terminó en `SUCCEEDED`.

## Evidencia y límites

El recorrido Kinesis → Bronze/Silver → Gold → Athena → Superset se probó con datos reales en AWS. La consulta inicial de Gold devolvió 100 órdenes, COP 14.561.000 en ventas y ticket promedio COP 145.610; una segunda ejecución conservó esos valores. Tras la rampa de 688.858 eventos, Gold terminó en `success` con 15.069 órdenes, COP 5.066.075.700 y 545.169 hechos enlazados a Bronze sin faltantes. Las pruebas de escenarios reales AWS se guardan en `entregable2/p4-infra-orquestacion/evidence/aws-scenarios/`: incluyen alertas y carritos de punta a punta, ventas por minuto, deduplicación, enriquecimiento, recuperación del DAG, SCD2 y monitoreo. El destinatario confirmó la recepción de los correos de RF-07 y RF-12.

La carga reveló dos problemas de Gold que no aparecieron en las muestras pequeñas: reaplicar un snapshot histórico sobre una dimensión ya versionada duplicaba claves SCD2, y crear un DataFrame Spark desde una lista local grande excedía los 128 MiB por tarea RPC. P4 corrigió la reconstrucción de la dimensión desde todos los snapshots y repartió los registros antes del `MERGE`; P3 conserva la responsabilidad de revisar y defender estas transformaciones. La consulta RNF-06 se corrigió para usar el nombre efectivo de la tabla Bronze y contar `event_id` únicos antes del cruce. El equipo debe revisar la evidencia junto con el código y poder explicar estas decisiones en la defensa.
