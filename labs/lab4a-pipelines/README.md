# Lab 4a · DAG de pipeline end-to-end (Airflow)

**Curso:** ST1630 Sistemas Intensivos en Datos — EAFIT
**Semana:** S11
**Peso:** parte del 30% de laboratorios (ver `../../docs/evaluacion.md`)
**Tiempo estimado:** 4-5 horas
**Entrega:** PR a este repo, ver sección "Entregable" abajo

## Objetivo

En el Lab 1b construiste a mano un pipeline Bronze → Silver → Gold con
PySpark y Delta Lake. Funcionó, pero tenía que correrlo tú, a mano, cada
vez. El equipo de datos de Nequi necesita que ese mismo tipo de pipeline
corra **automáticamente todos los días**, con reintentos si algo falla,
con detección de datos corruptos antes de que lleguen a producción, y
con una forma de ver de un vistazo si la corrida de ayer salió bien.
Esta semana orquestas ese pipeline con Apache Airflow.

Al terminar este lab deberías poder:
- **Implementar** un DAG de Airflow con tasks que dependen entre sí.
- **Diagnosticar** por qué un pipeline reintentado o backfilleado
  duplicaría datos si no se diseña con idempotencia en mente.
- **Verificar**, con un check de calidad de datos, que una partición de
  datos corrupta no avance silenciosamente hacia Silver/Gold.
- **Justificar** tus decisiones de diseño (schedule, catchup, qué
  check hace fallar el pipeline y cuál no) en `dag_design.md`.

## Prerequisito

Haber completado el Lab 1b (necesitas entender qué es Bronze/Silver/Gold
y qué hace un MERGE de Delta Lake — este lab asume que ya lo viviste una
vez a mano). **No** necesitas que tu clúster EMR ni tu bucket de S3 del
Lab 1a/1b sigan vivos: este lab usa un dataset propio, generado por
`datos/gen_dataset.py`, y corre 100% local dentro de Docker.

Verifica que tienes Docker y Docker Compose funcionando:

```bash
docker --version
docker compose version   # o `docker-compose --version` según tu instalación
```

Output esperado: ambos comandos devuelven un número de versión sin error.

## Parte 1 — Generar el dataset y levantar Airflow (30 min)

El dataset (`datos/ventas_colombia_lab4a.csv`) **no está en el repo** —
igual que el de Lab 1b, los `.csv` están en `.gitignore` a propósito
(no se suben datasets, se generan). Primero genéralo localmente:

```bash
cd labs/lab4a-pipelines
python3 datos/gen_dataset.py   # genera datos/ventas_colombia_lab4a.csv (determinista, semilla fija)
```

Luego levanta Airflow:

```bash
docker compose up airflow-init      # una sola vez: crea la BD y el usuario admin
docker compose up -d                # levanta webserver + scheduler en segundo plano
```

La primera vez, `airflow-init` también construye la imagen (instala
Java 17 + pyspark + delta-spark dentro del contenedor de Airflow) — eso
puede tardar varios minutos. Las siguientes veces es casi instantáneo.

Entra a **http://localhost:8080** (usuario: `admin`, clave: `admin`).
Deberías ver el DAG `ventas_colombia_pipeline` en la lista, en pausa
(ícono gris) — normal, `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True`.

**Verifica:** si `docker compose ps` no muestra `airflow-webserver` y
`airflow-scheduler` como `healthy`/`running`, revisa Troubleshooting
abajo antes de seguir.

## Parte 2 — Completa el DAG (2-3 horas)

Abre `dags/ventas_pipeline.py`. Tiene 4 bloques `# TODO` — son el
trabajo real de esta semana:

| TODO | Dónde | Qué resuelve |
|---|---|---|
| TODO 1 | `ingest_bronze` | Escritura idempotente de la partición Bronze del día |
| TODO 2/3 | `validate_bronze` (`check_nulos_criticos`, `check_rango_monto`) | Los dos checks de calidad que sí hacen fallar el pipeline |
| TODO 2 (transform) | `transform_silver` | Casts + filtro de devueltos + columna `anio_mes` |
| TODO 3 (MERGE) | `transform_silver` | El MERGE idempotente a Silver (el reto principal) |
| TODO 4 | `build_gold` | Las dos agregaciones de Gold |

El archivo explica en cada bloque qué hace falta y por qué. No necesitas
tocar nada fuera de los `# TODO`.

**Verifica tus checks de calidad sin levantar Airflow completo:**

```bash
docker compose exec airflow-scheduler pytest /opt/airflow/dags/tests/ -v
```

Si los 3 tests pasan, `check_nulos_criticos` y `check_rango_monto` están
bien implementados — eso no verifica `ingest_bronze`, `transform_silver`
ni `build_gold`, que solo puedes probar corriendo el DAG completo.

## Parte 3 — Corre el DAG (1 hora)

1. En la UI, activa el DAG `ventas_colombia_pipeline` (el interruptor a
   la izquierda del nombre).
2. Corre un backfill de las primeras 7 fechas del dataset:

   ```bash
   docker compose exec airflow-scheduler \
     airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
   ```

3. En la UI, abre el DAG y revisa el **Grid/Tree View**: las 7 corridas
   (una por fecha) deben tener sus 4 tasks en verde.
4. Abre el **Graph View** de cualquiera de esas corridas: confirma que
   las 4 tasks están conectadas en el orden `ingest_bronze →
   validate_bronze → transform_silver → build_gold`.

**Capturas que necesitas para tu entrega:**
- Grid/Tree View con las 7 corridas del backfill en verde.
- Graph View del DAG.
- Logs de la task `validate_bronze` de cualquier corrida exitosa
  (clic en la task → "Logs").

## Parte 4 — Provoca un fallo a propósito (30 min)

Necesitas ver, con tus propios ojos, qué pasa cuando `validate_bronze`
sí encuentra un problema. Edita temporalmente
`datos/ventas_colombia_lab4a.csv`: en varias filas de UNA fecha que
todavía no hayas corrido (por ejemplo `2026-01-08`), borra el valor de
la columna `total` en al menos un 6% de esas filas (deja la celda
vacía). Guarda el archivo.

Luego corre esa fecha específica:

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline
```

`validate_bronze` debería fallar (rojo en la UI) y `transform_silver`/
`build_gold` NO deberían ejecutarse para esa fecha. Guarda el log
completo de la task fallida — lo necesitas para `dag_design.md`.

Cuando termines, puedes regenerar el CSV limpio corriendo de nuevo
`python3 datos/gen_dataset.py` (no uses `git checkout` — el archivo no
está trackeado en git, así que no hay una versión committeada a la cual
volver; el script determinista es tu "revert").

## Parte 5 — Completa la documentación (30-45 min)

Copia las plantillas a tu carpeta de entrega y complétalas:
- `plantillas/dag_design_template.md` → `dag_design.md`
- `plantillas/bitacora_delegacion_template.md` → `bitacora_delegacion.md`

## Entregable

Pull request a este repo con esta estructura:

```
labs/lab4a-pipelines/entregas/<tu-usuario>/
├── dags/
│   └── ventas_pipeline.py          # tu DAG completo (los 4 TODO resueltos)
├── dag_design.md
├── bitacora_delegacion.md
└── capturas/
    ├── grid_view_backfill.png
    ├── graph_view.png
    └── log_validate_bronze.png
```

**No subas:** la carpeta `logs/` que genera Airflow localmente, el
contenido del volumen `lab4a-data` (son outputs regenerables, no
código), el `.csv` del dataset (ya está en `.gitignore`, igual que en
Lab 1b), ni credenciales de ningún tipo. No necesitas subir
`docker-compose.yml` ni el `Dockerfile` modificados — si no los tocaste,
no hace falta duplicarlos en tu entrega.

## Rúbrica

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| Idempotencia (`ingest_bronze` + MERGE de `transform_silver`) | 35% | Backfill repetido del mismo rango no duplica filas en Bronze ni en Silver; lo demuestra con evidencia (conteos antes/después) | Una de las dos capas es idempotente, la otra no, o la evidencia es solo narrada sin números reales | Ninguna de las dos escrituras es idempotente, o no hay evidencia de haberlo verificado |
| Checks de calidad (`validate_bronze`) | 25% | Los 2 checks "duros" detectan correctamente datos corruptos (demostrado con el fallo provocado de la Parte 4) y los tests de pytest pasan | Los checks existen pero no detectan el problema real, o los tests fallan | `validate_bronze` no bloquea el pipeline ante datos corruptos |
| Agregaciones de Gold | 15% | Las 2 tablas de Gold son correctas y se recalculan completas en cada corrida | Una de las dos tiene un error de lógica o de columnas | Gold no se ejecuta o las agregaciones no tienen sentido |
| `dag_design.md` | 15% | Las 6 secciones respondidas con criterio propio, citando evidencia real de tu propia ejecución (logs, conteos) | Respuestas genéricas o sin evidencia propia | Documento incompleto o copiado sin adaptar |
| Capturas y evidencia del backfill/fallo provocado | 10% | Las 3 capturas pedidas, legibles, mostrando exactamente lo que se pide | Faltan capturas o no muestran lo pedido con claridad | Sin capturas |

## Bitácora de delegación

Recuerda incluir tu bitácora al final de la entrega (ver
`../../docs/politica-ia.md`). Qué delegar vs. qué no, específico para
este lab:

| Tarea | ¿Se puede delegar por defecto? | Justificación |
|---|---|---|
| Sintaxis puntual de PySpark/Airflow (¿cómo se llama tal método?) | Sí | Troubleshooting repetitivo, bajo valor de aprendizaje |
| Boilerplate de Docker/Airflow ya dado en este repo | Sí (ya viene resuelto) | No es parte del ejercicio |
| Estrategia de idempotencia de `ingest_bronze`/`transform_silver` (TODO 1 y TODO 3) | **No** | Es el objetivo 1 de la semana — si un agente te lo resuelve, no vas a poder explicar en la defensa por qué evita la duplicación |
| Diseño de los checks de `validate_bronze` (TODO 2) | **No** | Es el objetivo 2 de la semana — la rúbrica evalúa que TÚ decidiste qué cuenta como dato corrupto |
| Redacción/formato de `dag_design.md` | Parcial | El criterio y la evidencia citada deben ser tuyos; pulir la redacción sí se puede delegar |

## Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| `docker compose up airflow-init` falla con error de permisos en `./logs` | El contenedor corre como un UID distinto al tuyo | `mkdir -p logs && chmod -R a+rwx logs` antes de levantar de nuevo |
| El DAG no aparece en la UI, o aparece con un ícono de error rojo junto al nombre | Error de sintaxis en `ventas_pipeline.py`, o te faltó completar un TODO y dejó una variable en `None` donde Airflow espera otra cosa al parsear el archivo | `docker compose exec airflow-scheduler airflow dags list-import-errors` te muestra el traceback exacto |
| `Failed to find the data source: delta` | La SparkSession no cargó bien la config de Delta | Confirma que `_spark_session()` no fue modificada — ese error no debería aparecer si usas el helper dado tal cual |
| El backfill se queda "running" muchísimo tiempo o el contenedor se queda sin memoria | Cada task levanta su propia JVM de Spark (4 por fecha) — en una máquina con poca RAM asignada a Docker esto puede ser lento | Sube la memoria asignada a Docker Desktop (Settings → Resources) a al menos 4GB, o corre el backfill de menos fechas a la vez |
| `check_nulos_criticos`/`check_rango_monto` siempre "pasan" aunque metas datos corruptos | Te quedaste con el `pass` del TODO sin reemplazarlo | Revisa que la función de verdad calcule el porcentaje/conteo y haga `raise ValueError(...)` cuando corresponda |

## Referencias

- [Documentación oficial de Airflow — Tasks y dependencias](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Documentación de Delta Lake — MERGE](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
- `../lab1b-batch/scripts/02_silver.py` — el MERGE original que este lab reutiliza, ahora dentro de un DAG
