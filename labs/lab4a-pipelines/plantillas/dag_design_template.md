# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a `entregas/<tu-usuario>/dag_design.md` y complétalo
> con evidencia de TU propia ejecución (conteos reales, logs reales) —
> no respuestas genéricas sobre cómo funciona Airflow en general.

## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:** → [tu respuesta aquí — piensa en cuándo y cómo el
equipo de Nequi consumiría las tablas de Gold de este pipeline]

**¿Por qué `catchup=False`?** → [tu respuesta aquí — explica qué pasaría
distinto si el DAG hubiera estado 30 días sin activarse y lo activaras
con `catchup=True`]

## Idempotencia — ¿qué significa y cómo la logré?

**Definición:** Un pipeline es idempotente si ejecutarlo N veces con la
misma entrada produce el mismo resultado que ejecutarlo 1 vez.

**Cómo la implementé en `ingest_bronze` (TODO 1):** → [tu respuesta aquí]

**Cómo la implementé en `transform_silver` (TODO 3, el MERGE):** → [tu
respuesta aquí — explica la condición de MERGE y por qué un `append`
simple no habría sido idempotente]

**Prueba de idempotencia que realicé:** → [describe cómo verificaste:
¿corriste el backfill de la misma fecha dos veces? ¿qué conteo de filas
tenía Bronze/Silver antes y después de la segunda corrida?]

```
[pega aquí los conteos reales de tu prueba — p. ej. salida de
`docker compose exec airflow-scheduler airflow tasks test ...` o los
logs de la task]
```

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | → [tu respuesta] | → [tu respuesta] |
| Rango de monto | `validate_bronze` | → [tu respuesta] | → [tu respuesta] |
| Cardinalidad de canal | `validate_bronze` | → [tu respuesta] | → [tu respuesta — nota que este NO lanza excepción, a diferencia de los otros dos; explica por qué eso tiene sentido] |

**¿Qué check añadirías en un pipeline de producción real?** → [al menos
2 ideas tuyas, distintas a los 3 ya implementados]

## Backfill — ¿qué es y cuándo se usa?

**Definición:** → [tu respuesta aquí]

**Comando que ejecuté:** → [pega el comando exacto que corriste]

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de,
por ejemplo, "la fecha de hoy" (`datetime.now()`)?** → [tu respuesta
aquí — piensa en qué pasaría con un backfill de fechas pasadas si
`ingest_bronze` usara la fecha real del sistema en vez de `ds`]

## Fallo provocado — log de `validate_bronze`

Pega aquí el log completo de la task cuando falló (Parte 4 del README):

```
[log aquí]
```

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue
a producción sin que nadie se entere?** → [tu respuesta aquí — piensa
en alertas, notificaciones, dashboards]

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**
→ [tu respuesta aquí]

**¿Qué te complica?** → [tu respuesta aquí — sé honesto, no tiene que
ser solo ventajas]
