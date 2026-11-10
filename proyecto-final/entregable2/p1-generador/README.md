# P1 · Generador, maestros, Superset y pruebas (E2)

Responsable: Juan Simón Ospina · RF-04 y pruebas de todos los RF.

Qué contiene este directorio:

| Carpeta | Qué es |
| --- | --- |
| `tiendacol_gen/` | Generador (`boto3 PutRecords` a Kinesis), maestros diarios, escenarios de prueba y verificadores |
| `contratos/` | JSON Schema de `clickstream`, `orders` y `stock_changes` (**propuesta para la Fase 0**) |
| `superset/` | Superset local con conector Athena y las consultas de cada gráfico |
| `tests/` | 36 pruebas del propio generador y de los verificadores (no requieren AWS) |

## Instalación

```powershell
cd proyecto-final/entregable2/p1-generador
pip install -r requirements.txt
python -m pytest -q          # ~20 s, sin AWS
```

## Uso

```powershell
# 1. Maestros del día -> archivos locales (RF-04). Con --bucket los sube a s3://BUCKET/landing/masters/
python -m tiendacol_gen masters --out out/masters --date 2026-11-08
python -m tiendacol_gen masters --out out/masters --date 2026-11-08 --bucket MI-BUCKET-LAKE

# 2. Tráfico continuo (archivo local o Kinesis)
python -m tiendacol_gen run --sink file --rate 500 --duration 60
python -m tiendacol_gen run --sink kinesis --stream tiendacol-events --rate 100 --duration 120

# 3. Rampa hasta 5.000 ev/s (RNF-01); imprime lo logrado por escalón y guarda el reporte
python -m tiendacol_gen run --sink kinesis --ramp 500,1000,2000,3000,5000 --step-seconds 60 --report out/rampa.json

# 4. Escenarios de prueba + verificación
python -m tiendacol_gen scenario list
python -m tiendacol_gen scenario rf05 --sink kinesis --stream tiendacol-events
python -m tiendacol_gen verify out/expected/expected_rf05.json --silver <tabla> --quarantine <tabla>
```

`--sink file|kinesis|null`. Las tablas de `verify` pueden ser `.csv`, `.jsonl`, `.parquet`, un
directorio Delta (`pip install deltalake`) o `athena://<base>?<SQL>` (`pip install pyathena`, y
`ATHENA_STAGING=s3://<bucket-ops>/athena-results/`).

Por defecto el generador inyecta 1 % de duplicados (copias idénticas, con retraso de 0-20 s),
1 % de eventos tardíos (`event_ts` 2-3 min atrás, **más allá del watermark de 1 min**) y 1 % de
malformados (campo ausente, tipo erróneo, tipo de evento desconocido o JSON truncado). Todo se
cuenta en el reporte (`stats`) para cuadrar contra Bronze/Silver. Ajustables con `--dup-rate`,
`--late-rate`, `--malformed-rate`.

## Mapa RF → prueba

| RF / RNF | Escenario (`scenario <n>`) | Verificación | Notas |
| --- | --- | --- | --- |
| RF-01 | `rf01` · 10.000 eventos únicos | `verify` · idénticos en Bronze | |
| RF-02 | `rf02` · 100 órdenes + 20 cambios de estado | `verify` | |
| RF-03 | `rf03` · 50 cambios de stock | `verify` · stock por producto | stock = inventario maestro + Σ delta |
| RF-04 | `masters` | `verify_rf04(manifest, conteos)` + `check_load_deadline` | el manifiesto se sube al final |
| RF-05 | `rf05` · 9.800 únicos + 100 duplicados + 100 malformados | `verify` · Silver 9.800, cuarentena 100 | |
| RF-06 | (cualquiera) | `verify_rf06(masters, silver)` · muestra de 1.000 | |
| RF-07 | `rf07` · 1 unidad/12 s, stock 320 → alerta tras la venta #65 | `verify` con `--alerts` JSONL | **tiempo real, ~15 min** |
| RF-08 | `rf08` · sesiones A/B/C + latido | `watch_dynamodb_carts` + `verify_rf08` | **tiempo real, ~40 min** |
| RF-09 | `rf09` · 20 órdenes en un mismo minuto | `verify` (valores) y `seen_at` (latencia) | **tiempo real** |
| RF-10/11 | cualquier tráfico guardado en JSONL | `funnel_oracle` / `sales_oracle` + `compare_frames` | oráculo en pandas calculado "a mano" |
| RF-12 | — | **manual**: ver abajo | falla inyectada |
| RF-13 | `rf13` · A y B juntos en 50 órdenes | `verify` · B primero en la lista de A | opcional |
| RNF-01 | `run --ramp` | reporte de la rampa + IteratorAge en CloudWatch | |
| RNF-03 | `rnf03` · 10.000 + 100 reenviados | `verify` · Bronze 10.100, Silver 10.000 | |
| RNF-06 | — | `verify_rnf06(gold, bronze)` | |

Detalles que importan al reproducirlos:

- Para repetir un escenario en el mismo lago, usar `--scenario-seed` con un número diferente; así
  se generan nuevos `event_id` y la deduplicación de Silver no descarta la repetición.
- **RF-08 necesita tráfico de fondo.** El watermark de Spark solo avanza con eventos nuevos (E1,
  ADR-2); el escenario emite un `page_view` cada 10 s de un usuario de ruido. No lo apagues.
- **Fixtures `TEST-*`**: `masters` incluye 5 productos y 4 clientes de prueba con stock/precio/ciudad
  conocidos (`TEST-RF07`, `TEST-RF09-A/B`, `TEST-RF13-A/B`, `U-TEST-A/B/C/NOISE`). Los escenarios los
  usan para que el esperado se pueda calcular a mano. `--no-fixtures` los quita (no hacerlo antes de probar).
  Hay que correr `masters` y cargarlos **antes** de los escenarios para que el enriquecimiento los encuentre.
- **RF-07 · oráculo.** `first_alert_sale` evalúa tras cada venta: velocidad = unidades en los últimos
  15 min / 15; cobertura = stock / velocidad; alerta si < 60 min. Con los valores por defecto sale en
  la venta #65 (cuenta a mano: `(320−k)·15/k < 60` ⇔ k > 64). **P2 debe confirmar que su fórmula en la
  ventana de 15 min es la misma** (en particular, qué velocidad usa antes de que la ventana se llene).
- **RF-09** se lanza ~3 s antes del siguiente minuto calendario; todas las órdenes caen dentro de ese minuto.

### RF-12 (manual, no automatizable aquí)

1. Forzar un fallo en la carga de maestros (p. ej. subir un CSV con columnas inválidas a `landing/masters/`).
2. Comprobar en Airflow: pasos dependientes en `upstream_failed` y notificación enviada.
3. Corregir el archivo, reintentar el DAG: el flujo termina completo sin intervención.

### RNF-05 (manual)

Detener el job de Glue Streaming durante `scenario rnf03`, reiniciarlo, y verificar con `verify`
que Bronze conserva 10.100 y Silver 10.000.

## Superset

`superset/docker-compose.yml` levanta Superset local con `pyathena` si Docker está disponible.
El despliegue AWS de P4 también crea una EC2 de Superset con rol IAM y sin puertos entrantes;
`setup_superset.py` registra la conexión, cuatro datasets, cuatro gráficos y el dashboard.
Para la variante local: copiar `.env.example` a `.env` (credenciales de la cuenta: **no subirlas a Git**),
ejecutar `docker compose up -d --build`, abrir `http://localhost:8088` y crear la conexión

```
awsathena+rest://@athena.us-east-1.amazonaws.com:443/tiendacol_gold?s3_staging_dir=s3://<BUCKET-OPS>/athena-results/&work_group=tiendacol-analytics
```

`superset/datasets.sql` trae las consultas de ventas por minuto, embudo, ventas/ticket y frescura,
alineadas con el DDL de P3. En el despliegue EC2, el rol de instancia evita claves AWS estáticas.

## Contratos — decisiones que propongo cerrar en la Fase 0

1. **Un stream o tres.** El diagrama muestra un único Kinesis; el generador acepta ambos
   (`--stream` o `--stream-map clickstream=…,orders=…,stock_changes=…`). Con un solo shard en la demo
   un stream único basta; con 6 shards la clave de partición reparte la carga. La clave de cada tipo
   está en el esquema (`user_id`/`session_id`, `order_id`, `product_id`).
2. **Envoltorio común**: `event_id` (UUID v4), `event_type`, `event_ts` (UTC, ms, sufijo `Z`),
   `schema_version`. El `event_ts` es el tiempo del evento; Bronze debe añadir `ingested_at`,
   `shard`/`sequence_number` aparte (RNF-06).
3. **Stock como delta**, no como valor absoluto: duplicados descartados por `event_id` y reordenar no
   cambia el resultado. Stock inicial = maestro `inventory`.
4. **`city`/`department` viajan en el evento** (contexto de sesión, también para anónimos), como pide el E1.
5. **Compra = 3 registros**: `purchase` (clickstream, con `order_id`), `order_created` (orders) y un
   `stock_change(sale)` por línea. P2 no debe contar la compra dos veces: para RF-09 usar `orders`.
6. **Maestros**: `landing/masters/<entidad>/dt=<AAAA-MM-DD>/<entidad>.csv` (catálogo también en JSON con
   `--products-format json`) y `_manifest_dt=<día>.json` subido al final como señal de carga completa.
   Los atributos de producto varían por categoría y van como JSON en la columna `attributes`.
7. **Clientes seudonimizados** (`U` + hash): el generador no produce PII.

## Estado y límites (para no sorprenderse)

- **Probado**: generador, maestros, escenarios y verificadores con 36 pruebas locales; Kinesis con un
  cliente simulado (reintento de registros rechazados, límites de 500 registros / 5 MiB, ruteo por stream).
  Sin red, el generador sostiene ≈10.000 ev/s.
- **Integración AWS desde P4:** `PutRecords` real, Bronze/Silver/Gold, Athena y el conector
  PyAthena de Superset se probaron en la cuenta de TiendaCol. La evidencia de escenarios AWS se
  guarda en `../p4-infra-orquestacion/evidence/aws-scenarios/`; la rampa RNF-01 se mide por separado.
- **Riesgo del Learner Lab**: en el Lab 2b el rol `voclabs` no tenía `kinesis:CreateStream`
  (`AMBIENTE.md`, 2026-09-13). Es lo primero que debe confirmar P4 en la Fase 1; sin Kinesis no hay
  `--sink kinesis` y habría que replantear el transporte.
- Las pruebas de tiempo real (RF-07/08/09) dependen de cómo P2 configure triggers y watermarks; el
  generador solo garantiza entradas correctas y un esperado calculable.
