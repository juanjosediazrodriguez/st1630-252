# Estado de verificación · TiendaCol E2

La evidencia reproducible de AWS está en [`evidence/aws-scenarios/`](../../entregable2/p4-infra-orquestacion/evidence/aws-scenarios/). Los archivos `observed_*.json` son resultados medidos; los `expected_*.json` son los oráculos del generador P1. Los oráculos grandes se regeneran con las semillas indicadas en cada resultado y se excluyen de Git.

| Requisito | Comprobación en AWS | Resultado |
| --- | --- | --- |
| RF-01/02/03 | Eventos de clickstream, órdenes y stock en Kinesis/Bronze | Pasó |
| RF-04 | Snapshot diario de cinco maestros antes de las 05:00 COT | Pasó |
| RF-05/06 | Cuarentena, deduplicación y enriquecimiento con maestros | Pasó |
| RF-07 | Alerta de inventario, SQS → Lambda → SNS | Pasó; destinatario confirmó correo |
| RF-08 | Carrito sin compra durante 30 minutos, SQS → Lambda → DynamoDB | Pasó |
| RF-09 | Ventas por minuto en Delta/Athena antes de 2 minutos | Pasó |
| RF-10/11 | Embudo y ventas/ticket promedio en Gold mediante Athena y Superset | Pasó |
| RF-12 | Fallo controlado de maestros, dependencias bloqueadas y recuperación | Pasó; destinatario confirmó correo |
| RNF-01 | Rampa real de 500 a 5.000 ev/s con seis shards | Cinco escalones sostenidos; máximo 4.991,7 ev/s; IteratorAge máximo 84 s en la ventana medida |
| RNF-03 | 10.100 Bronze, 10.000 Silver únicos | Pasó |
| RNF-06 | Hechos Gold enlazados a `event_id` de Bronze | 5.308/5.308 antes de la actualización Gold posterior a la carga |
| SCD2 | Dos versiones para 25 productos, una vigente por producto | Pasó |
| Monitoreo | Alarmas DLQ/IteratorAge, fallo Glue→SNS, Budget | Pasó |
| Tablero | Cuatro gráficos consultados por la API de Superset | Pasó |

El diagrama implementado está en [arquitectura_final.md](arquitectura_final.md). El despliegue se reproduce desde la raíz con `deploy.ps1` o `deploy.sh` y los comandos del [manual P4](../../entregable2/p4-infra-orquestacion/README.md). Las diapositivas y el ensayo de la defensa quedaron para el equipo, según el acuerdo de trabajo.
