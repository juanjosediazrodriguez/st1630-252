# Arquitectura final implementada · TiendaCol E2

```mermaid
flowchart LR
    P[Generador Python] -->|PutRecords| K["Kinesis Data Streams<br/>1 shard demo · 6 prueba"]
    K --> GS["Glue Streaming<br/>Spark Structured Streaming"]
    M["Maestros diarios<br/>05:00 COT"] --> L[S3 landing/masters]
    L --> GS
    GS --> B[S3 Bronze Delta]
    GS --> S[S3 Silver Delta]
    GS --> Q[S3 quarantine Delta]
    S --> GB[Glue Batch horario]
    L --> GB
    GB --> G["S3 Gold Delta<br/>hechos y dimensiones SCD2"]
    GS --> AM["S3 Gold Delta<br/>agg_sales_minute"]
    G --> C[Glue Data Catalog]
    AM --> C
    C --> A[Amazon Athena]
    A --> BI["Apache Superset en EC2<br/>acceso por SSM"]
    GS --> SA[SQS alertas]
    SA --> LA[Lambda alertas]
    LA --> SNS[SNS correo]
    GS --> SC[SQS carritos]
    SC --> LC[Lambda carritos]
    LC --> D[DynamoDB carritos]
    SA --> DA[DLQ alertas]
    SC --> DC[DLQ carritos]
    AF["Airflow en EC2<br/>bootstrap · health · masters · Gold · stop"] --> GS
    AF --> GB
    CW["CloudWatch<br/>DLQ · IteratorAge · fallos Glue"] --> SNS
```

CloudFormation despliega los recursos de la región `us-east-1` y un presupuesto con alertas. S3 y Kinesis usan KMS administrado por AWS; SQS usa SSE administrado por SQS. Las dos EC2 tienen roles IAM separados y no admiten tráfico entrante. El acceso a Airflow y Superset se hace mediante Session Manager.

Cada tabla Gold tiene un único escritor. Glue Streaming escribe `agg_sales_minute` para el límite de latencia de RF-09; Glue Batch escribe las dimensiones y demás tablas de hechos. Esta es la modificación respecto a la frase «Gold tiene un único escritor: AWS Glue Batch» del [diagrama de diseño](<../../TiendaCol_Arquitectura_AWS_2026.drawio (1).pdf>).

`tiendacol_stop_demo` detiene Glue Streaming. `stop_costs.py` detiene ambas EC2 y los jobs; `destroy_demo.py` elimina el stack y el stream Kinesis cuando termine la evaluación. Los buckets S3 se conservan por `DeletionPolicy: Retain`.
