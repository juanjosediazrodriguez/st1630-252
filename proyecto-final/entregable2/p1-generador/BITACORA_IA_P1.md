# Bitácora de IA — P1 (E2)

Herramienta: Claude Code. P4 consolida la bitácora del E2; estas son las filas de P1.

| Tarea | ¿Delegado? | Justificación / cómo se verificó |
| --- | --- | --- |
| Clonar el repo y crear `e2-tiendacol` desde `e1-tiendacol` | Sí | Tarea mecánica |
| Decisiones de contrato (un stream vs. tres, stock como delta, envoltorio del evento) | Parcial | El agente propuso; **P1 debe validar con el equipo en la Fase 0** (README, sección Contratos) |
| Código del generador, maestros, sinks y runner | Sí | Borrador del agente; revisado y probado localmente (36 pruebas) |
| Escenarios y verificadores de los RF | Sí | El agente derivó los valores esperados del E1; **P1 revisa que cada cifra coincida con el texto del RF** (p. ej. 9.800/100 en RF-05, venta #65 en RF-07) |
| Consultas SQL de Superset | Parcial | Propuesta sobre nombres supuestos de Gold; pendiente de alinear con P3 |
| Ejecución real contra AWS | No | Aún no realizada |
