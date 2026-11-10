# Bitácora de IA — P2 (E2)

Herramienta: Claude Code. Opus 5.5 como líder (diseño, reparto y revisión) y dos agentes Sonnet como desarrolladores.
P4 consolida la bitácora del E2; estas son las filas de P2.

| Tarea | ¿Delegado? | Justificación / cómo se verificó |
| --- | --- | --- |
| Revisar lo nuevo de P1 y el diagrama del E2, y comparar con el E1 | Sí | El agente leyó contratos, verificadores y diagrama. **P2 confirmó** las tres decisiones abiertas que se llevan al grupo: un stream, `sales_cop` y `agg_sales_minute` escrita por el streaming |
| Diseño de P2 (`DISENO.md`): topología de 5 consultas, contratos de tablas y mensajes SQS | Parcial | El líder lo propuso a partir de los valores compartidos del E1. **P2 revisa** que cada umbral coincida con el E1 (30 s, watermarks de 1 y 3 min, 60/15 min, COP 500.000, 30 min) |
| Código del job de Glue Streaming (Q1 ingesta, Q2 stock, Q5 ventas por minuto) | Sí | Desarrollador 1. Probado en la imagen oficial de Glue 4.0 con los escenarios y verificadores de P1 (RF-01, 03, 05, 06, 09, RNF-03) |
| Código de Q3 alertas (RF-07) y Q4 carritos (RF-08) | Sí | Desarrollador 2, a partir de los borradores del desarrollador 1. Alerta en la venta #65 (no en la #64), una sola alerta, U-TEST-A sí y B/C no |
| Revisión del código y ajuste del `MERGE` de Silver (poda de particiones con literales de fecha) | Parcial | El líder revisó ingesta, validación, stock, ventas, alertas y carritos, y corrió la suite completa: 56 pruebas pasan y 5 se omiten a propósito (16 min 39 s en la imagen de Glue 4.0, 2026-11-09) |
| Evidencia de la fuente de Kinesis en Glue 4.0 (formato `kinesis` y columnas) | Sí | Inspección de los jars de la imagen y `readStream...load().schema`. **Falta la prueba de humo contra un stream real** |
| Ejecución real contra AWS (Kinesis, SQS, latencias de la demo) | No | Aún no realizada: depende de la infraestructura de P4 y del permiso de Kinesis en el Learner Lab |
| Commit y push de la rama `e2-p2-streaming` | Sí | A pedido explícito de P2 (la regla del equipo es que lo haga el humano). Antes se revisó que no entraran credenciales ni archivos ajenos |
