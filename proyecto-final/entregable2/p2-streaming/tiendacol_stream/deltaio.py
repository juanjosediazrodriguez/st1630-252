"""Escrituras Delta reutilizables: append idempotente, MERGE por SQL y trigger de las consultas."""
from __future__ import annotations

import json

from pyspark.sql import DataFrame

from .config import Config
from .util import delta_ident, read_text, view_name


def trigger_kwargs(cfg: Config, seconds: int) -> dict:
    """availableNow en pruebas (procesa lo pendiente en varios micro-batches y termina); si no, cadencia fija."""
    return {"availableNow": True} if cfg.run_once else {"processingTime": f"{seconds} seconds"}


def query_id(spark, checkpoint: str) -> str:
    """Id estable de la consulta: Spark lo guarda en `<checkpoint>/metadata` y no cambia al reiniciar con el
    mismo checkpoint. Sirve de `txnAppId`: si se borra el checkpoint (otro id), el contador de lotes vuelve a
    empezar de 0 sin chocar con las versiones que Delta recuerda de la corrida anterior."""
    try:
        return json.loads(read_text(spark, f"{checkpoint}/metadata"))["id"]
    except Exception:  # noqa: BLE001 - sin metadata (p. ej. prueba directa de process_batch)
        return "local"


def read_silver_stream(spark, cfg: Config) -> DataFrame:
    """Silver como fuente de streaming (Q2-Q5). Sirve porque el único escritor, Q1, solo inserta: Delta lo
    registra como archivos nuevos (sin reescrituras) y la fuente Delta lo lee como si fuera append.
    `maxFilesPerTrigger` limita el tamaño del micro-batch (en pruebas, para tener varios lotes)."""
    r = spark.readStream.format("delta")
    if cfg.delta_max_files_per_trigger:
        r = r.option("maxFilesPerTrigger", cfg.delta_max_files_per_trigger)
    return r.load(cfg.silver)


def append_idempotent(df: DataFrame, path: str, app_id: str, version: int) -> None:
    """Append con `txnAppId`/`txnVersion` de Delta: si Glue reintenta el micro-batch, Delta reconoce que la
    versión `version` de `app_id` ya se escribió y descarta la segunda escritura (idempotencia)."""
    (df.write.format("delta").mode("append")
       .option("txnAppId", app_id).option("txnVersion", int(version)).save(path))


def merge(source: DataFrame, path: str, on: str, *, update_set: dict[str, str] | None = None,
          insert: bool = True) -> dict:
    """MERGE por SQL (en la imagen de Glue no existe el módulo Python `delta`). `t` es el destino y `s` el origen.

    Sin `update_set` el MERGE es solo de inserción: Delta lo registra como archivos nuevos, sin reescrituras,
    y por eso la tabla puede seguir leyéndose como fuente de streaming.
    """
    # Dentro de foreachBatch el DataFrame del lote vive en una sesión JVM clonada: la vista temporal se registra
    # ahí, así que el SQL debe correr en esa misma sesión (no en la `spark` global de Python).
    jsession = source._jdf.sparkSession()
    view = view_name("src")
    source.createOrReplaceTempView(view)
    sql = f"MERGE INTO {delta_ident(path)} AS t USING {view} AS s ON {on}"
    if update_set:
        sql += " WHEN MATCHED THEN UPDATE SET " + ", ".join(f"t.{k} = {v}" for k, v in update_set.items())
    if insert:
        sql += " WHEN NOT MATCHED THEN INSERT *"
    try:
        rows = DataFrame(jsession.sql(sql), source.sparkSession).collect()
    finally:
        jsession.catalog().dropTempView(view)
    return rows[0].asDict() if rows else {}
