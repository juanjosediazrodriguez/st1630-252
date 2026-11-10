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

## Integración y evidencia posterior

- Se probó el recorrido real Kinesis → Glue Streaming → Bronze/Silver/Gold → Athena → Superset. El dashboard de Superset está en otra EC2, con rol IAM propio y acceso mediante Session Manager, sin puertos entrantes.
- RF-01/02/03/05/06/07/08/09/12, RNF-03 y SCD2 tienen verificaciones reproducibles y resultados observados en `evidence/aws-scenarios/`. El destinatario confirmó por correo las notificaciones RF-07 y RF-12. RF-08 se midió durante su ventana de 30 minutos con tráfico de fondo para avanzar el watermark.
- En RF-12 se introdujo un maestro inválido, se verificaron tareas dependientes bloqueadas, se restauró el objeto S3 y la siguiente ejecución completó las cuatro tareas. El script de prueba restaura S3 en `finally`.
- Se desplegaron alarmas de DLQ y de atraso de Kinesis, un evento de fallo Glue a SNS y un presupuesto de USD 25. Se archivaron logs de Airflow en S3 ops.
- Para RNF-01, un intento directo de cambiar Kinesis de 1 a 6 shards falló porque AWS limita cada actualización a un máximo del doble. `deploy.py` ahora escala por etapas (1 → 2 → 4 → 6) y devuelve el stream por etapas después de la prueba.
- La rampa de cinco minutos emitió 688.858 eventos sin errores de `PutRecords`. Las tasas observadas fueron 499,2; 998,3; 1.996,7; 2.995,0 y 4.991,7 eventos/s durante 60 segundos por escalón. CloudWatch registró un máximo de 84 segundos de `GetRecords.IteratorAgeMilliseconds` en la ventana medida; el reporte íntegro es `observed_rnf01.json`.
- `tiendacol_gold_hourly` mantuvo 100 órdenes y COP 14.561.000 en una segunda ejecución sobre la muestra inicial. El verificador `verify_gold_aws.py` consulta RF-10, RF-11 y RNF-06 en Athena. Se corrigió en la consulta de trazabilidad el nombre real de la tabla Bronze y se deduplican sus `event_id` antes de cruzar con Gold.
- La primera ejecución Gold sobre la rampa se detuvo tras más de 12 minutos de trabajo en memoria. La siguiente descubrió que reaplicar snapshots antiguos sobre una `dim_product` ya versionada creaba claves SCD2 repetidas; se pasó a reconstruir la dimensión desde todos los snapshots persistidos. Una tercera corrida llegó a los hechos, donde Spark superó el límite RPC de 128 MiB al serializar una partición local de 144 MiB. Se repartió la fuente en grupos de hasta 10.000 registros y se aumentó el número de archivos de salida solo para tablas grandes. El job pasó a `MaxRetries=0`, límite 30 minutos, y el DAG espera hasta 35 minutos.
- La ejecución Gold corregida completó las nueve tablas en unos siete minutos. Athena registró 15.069 órdenes, COP 5.066.075.700 y 545.169 hechos con vínculo Bronze (0 sin vínculo); SCD2 mantuvo 25 productos con historial de dos versiones. El tablero de Superset respondió en sus cuatro gráficos.
- Tras reducir Kinesis de seis a un shard, `IteratorAge` aumentó temporalmente por lecturas de shards antiguos y activó la alarma. Un envío de 99 eventos nuevos produjo commits Bronze/Silver y la métrica volvió a 0; se verificaron 720.655 filas Bronze y 707.748 Silver. No se necesitó reiniciar el job.

Antes de cada sesión se inicia Airflow y Superset; al terminar se detienen Glue Streaming y ambas EC2. Kinesis permanece activo hasta eliminar el stack después de la evaluación.
