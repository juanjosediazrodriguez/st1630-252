# Reparto de trabajo — TiendaCol en tiempo real (E2)

Con la arquitectura aprobada, el reparto del E1 ya no sirve: cambian los componentes y cambia quién escribe Silver. El proyecto se divide en cuatro frentes, uno por integrante, con un bloque previo de contratos que hacemos los cuatro juntos.

## Qué cambió frente al E1

- **Kafka → Kinesis Data Streams** (1 shard en la demo, 6 en la prueba de 5.000 ev/s).
- **Spark en EC2 → AWS Glue**: Glue Streaming para tiempo real y Glue Batch para el horario.
- **La EC2 solo corre Airflow.** Es plano de control: arranca, supervisa y detiene los jobs de Glue por API. No ejecuta Kafka ni Spark.
- **Nuevos componentes:** SQS con una DLQ por caso de uso, Lambda para alertas y carritos, CloudWatch, KMS, Budgets y CloudFormation (IaC).
- **Silver lo escribe Glue Streaming**, no el batch. Por eso RF-05 y RF-06 pasan de P3 a P2.
- **Gold tiene un único escritor** (Glue Batch), con la excepción a decidir en la sección 5.

## Reparto por integrante

Cada RF tiene un responsable, salvo el RF-08, que se reparte entre P2 (detección en streaming) y P4 (publicación en DynamoDB).

| Integrante | Qué implementa | RF |
| --- | --- | --- |
| **P1 · Juan Simón Ospina** | Generador Python (`boto3 PutRecords` a Kinesis) con duplicados, eventos tardíos y subida de carga hasta 5.000 ev/s. Maestros diarios (catálogo, clientes, vendedores) a `landing/masters/`. Tableros de Superset. Pruebas de la columna "Cómo se verifica" de todos los RF. README, bitácora de IA, PR y control de costos. | RF-04 · pruebas de todos |
| **P2 · Sebastián Durán** | Streams de Kinesis y Glue Streaming: Bronze en append, validación, cuarentena, deduplicación y enriquecimiento hacia Silver. Stock como estado, ventana de 15 min (RF-07), sesión de 30 min (RF-08) y agregado por minuto (RF-09). Publica los eventos de decisión en SQS. | RF-01, 02, 03, 05, 06, 07, 08, 09 |
| **Juan José Díaz Rodríguez** | **P3** | **AWS Glue Batch horario:** dimensiones con historial de cambios (**SCD Tipo 2**), tablas de hechos `fact_sales` y `fact_funnel_event`, métricas de embudo de conversión y ventas. Registro de tablas en AWS Glue Data Catalog y consultas analíticas en Amazon Athena. | **RF-10, RF-11** · *(RF-13 opcional si hay tiempo)* |
| **Daniel Arcila Salazar** | **P4** | **Infraestructura como Código (IaC):** CloudFormation (S3, Kinesis, Glue, SQS + DLQ, SNS, DynamoDB, IAM, KMS, CloudWatch, Budgets). Funciones Lambda para alertas y carritos. Airflow en EC2 con los DAGs de bootstrap, health, maestros, horario y stop demo. `README.md`, bitácora consolidada de IA, PR y control de costos. | **RF-12** · Publicación de **RF-08** |

P2 queda con la carga más pesada del streaming y P3 con la lógica más pesada de Gold. P1 compensa con las pruebas y puede apoyar a P2 en RF-08 y RF-09.

## Dependencias entre integrantes

Las flechas van de quien entrega a quien lo necesita.

&#91;embedded content: dependencias entre integrantes · 7 entregas\]

- **P4 bloquea a todos al inicio.** Sin el stack base (S3, Kinesis, IAM) nadie prueba en AWS; mientras tanto el resto avanza en local con archivos y mocks.
- **P2 es el cuello de botella.** Su Silver alimenta a P3 y sus eventos de decisión en SQS alimentan a las Lambdas de P4. Debe entregar pronto una versión mínima de Silver.
- **P1 es independiente al inicio, pero crítico al final:** sus pruebas deciden si pasan los RF. Sus maestros diarios también alimentan el enriquecimiento de P2 y las dimensiones de P3.
- **P3 empieza con datos sintéticos** y se conecta al Silver real cuando P2 lo tenga.
- **P4 con Airflow** arranca y supervisa los jobs de P2 y P3.

## Orden de trabajo

La Fase 0 es lo único que hacemos los cuatro juntos; sin ella, los demás frentes se bloquean entre sí.

1. **Fase 0 · Contratos (primeros 2-3 días, los 4).** JSON de cada evento (`clickstream`, `orders`, `stock_changes`) con `event_id`; rutas de S3 (`bronze/`, `silver/`, `quarantine/`, `gold/`, `landing/masters/`); DDL de Silver y Gold; formato de los mensajes de SQS (`alert_id`, y `user_id` más valor del carrito).
2. **Fase 1 · Esqueleto de punta a punta.** Un evento del generador llega por Kinesis a Bronze en S3. P4 prueba primero que el Learner Lab permita Glue, Lambda, SQS, DynamoDB y CloudFormation; si algo está bloqueado, hay que saberlo ahora.
3. **Fase 2 · Lógica de negocio.** P2 construye streaming y Silver; P3 construye Gold con datos sintéticos; P4 las Lambdas y los DAG; P1 las pruebas de RF-01 a RF-09.
4. **Fase 3 · Integración.** Gold sobre el Silver real, Athena, Superset, README y DAG completos. Alguien que no escribió el código levanta todo desde cero.
5. **Fase 4 · Demo y defensa.** Ensayo completo el día anterior y simulacro de preguntas cruzadas, para que cada uno sepa defender lo que no implementó.

## Puntos a decidir entre nosotros

- [ ] **Quién escribe `agg_sales_minute`.** El diagrama dice que Gold tiene un único escritor (Glue Batch), pero el RF-09 exige menos de 2 min y eso solo lo logra el streaming. Propuesta: Streaming escribe esa tabla y Batch el resto, justificado como "un writer por tipo de tabla", como indica la nota del diagrama. Confirmarlo con el profesor.
- [ ] **Costos.** Glue Streaming cobra mientras corre y Kinesis no se puede pausar. P4 deja listos el DAG `stop demo` y el borrado del stream efímero antes de la primera prueba larga. Ningún job queda encendido entre sesiones.
- [ ] **Clave de partición del RF-08.** Usar `user_id` en el stream de clickstream, para conservar el orden por usuario y saber si hubo compra después del carrito.
- [ ] **RF-13 (opcional).** Solo si P3 termina Gold antes de la integración.
