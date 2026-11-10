# Bitácora de IA · P4

Responsable: Daniel Arcila Salazar. Se usó Codex para inspeccionar contratos P1–P3, generar CloudFormation, funciones Lambda, DAGs, scripts operativos y documentación, y ejecutar pruebas en la cuenta AWS del equipo. La implementación y las decisiones deben ser revisadas y defendidas por el equipo.

## Decisiones y verificación

- CloudFormation despliega el plano de datos y de control en `us-east-1`. El script `deploy.py` crea primero presupuesto y recursos base, sube artefactos y luego activa aplicaciones y Airflow.
- Glue Streaming inicia solo desde `tiendacol_bootstrap` después de cargar maestros. Airflow corre en EC2 por Docker, sin puertos entrantes y con rol IAM de instancia.
- Las Lambdas consumen SQS con DLQ y respuesta parcial; carrito usa upsert por `user_id` y alertas registran `alert_id`. Una caída entre SNS Publish y el marcador de DynamoDB puede repetir una notificación: la garantía real es *at-least-once*.
- Se probó SQS → Lambda → DynamoDB/SNS con mensajes aislados y se limpiaron los ítems de prueba.
- El generador P1 envió 598 eventos a Kinesis en 30 segundos; Bronze y Silver registraron commits Delta. El escenario RF-13 envió 300 registros adicionales.
- El primer Glue Batch falló por `pandas.Timestamp` no aceptado por `TimestampType` con esquema explícito. Se normalizaron los registros en `p3-gold-batch/tiendacol_gold/job.py`; las 7 pruebas de P3 pasaron y Glue Batch terminó en `SUCCEEDED`.
- El registro en Athena requirió agregar permisos al rol de Airflow y omitir `CREATE DATABASE`, porque CloudFormation ya crea ambas bases. La consulta de ventas confirmó 100 órdenes, COP 14.561.000 y ticket promedio COP 145.610.
- La cuenta nueva devolvió `KMS NotFoundException` para la clave administrada de DynamoDB durante el primer intento. Tras propagarse las claves, se actualizó el stack con KMS administrado por AWS para S3, Kinesis y DynamoDB. SQS conserva SSE-SQS. El script ahora espera a que KMS esté listo antes de crear recursos.

## Pendientes de la demo

- El DAG `tiendacol_gold_hourly` terminó en `success` y una segunda ejecución mantuvo 100 órdenes y COP 14.561.000 de ventas (idempotencia sobre la muestra).
- Ejecutar escenarios RF-07 y RF-08 en tiempo real si se van a mostrar sus latencias; RF-08 requiere unos 40 minutos y tráfico de fondo para avanzar el watermark.
- Detener Glue Streaming y EC2 entre sesiones. Kinesis permanece activo hasta eliminar el stack.
