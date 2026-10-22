# bitacora_delegacion.md — Lab 4a

**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Ver `../../../docs/politica-ia.md` para la política completa. Copia
> este archivo a `entregas/<tu-usuario>/bitacora_delegacion.md` y
> complétalo con tus propias filas — las de ejemplo de abajo son una
> guía de qué tan específica debe ser cada justificación, no algo para
> dejar tal cual.

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Schedule interval elegido (`@daily`) | _(Sí/No/Parcial)_ | _(ejemplo de nivel de detalle esperado: "No — decidí @daily yo mismo pensando en que Nequi revisa el dashboard cada mañana; le pregunté a un agente si @daily era sintaxis válida de cron, nada más")_ |
| Decisión de usar `catchup=False` | | |
| Estrategia de idempotencia en `ingest_bronze` (TODO 1) | | |
| Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3) | | |
| Diseño de los checks de `validate_bronze` (TODO 2) — qué cuenta como dato corrupto, qué umbral usar | | |
| Threshold del check de nulos (5% — ¿tiene sentido para este dataset?) | | |
| Agregaciones de `build_gold` (TODO 4) | | |
| Redacción de `dag_design.md` | | |
| Troubleshooting de Docker/Airflow (si tuviste algún problema de entorno) | | |
