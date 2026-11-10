"""Q1 · Ingesta: fuente -> Bronze (todo), cuarentena (rechazados) y Silver (válidos, únicos, enriquecidos).

Es la única consulta que lee Kinesis (un shard admite 5 `GetRecords`/s compartidos; cinco lectores se
estorbarían) y la única que escribe Bronze, cuarentena y Silver (un escritor por tabla).
No usa watermark: un evento tardío (2-3 min en las pruebas de P1) entra igual a Silver.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager

from pyspark.sql import DataFrame, functions as F

from .config import Config
from .deltaio import append_idempotent, merge, query_id, trigger_kwargs
from .enrich import enrich
from .masters import MastersHolder, get_holder
from .parse import parse_events, split_valid, to_quarantine, typed_events
from .sources import read_source
from .util import BOGOTA, now_lit

log = logging.getLogger("tiendacol.ingesta")


@contextmanager
def _timed(stats: dict, step: str):
    """Segundos por paso del micro-batch: sirve para ver dónde se va la latencia (presupuesto de DISENO §1)."""
    t0 = time.time()
    try:
        yield
    finally:
        stats.setdefault("seconds", {})[step] = round(time.time() - t0, 2)


def bronze_frame(parsed: DataFrame) -> DataFrame:
    """Bronze conserva TODO lo que llegó (duplicados y malformados incluidos): RNF-03 pide 10.100 para 10.000 únicos."""
    m = F.col("_m")
    return parsed.select("raw", m.getItem("event_id").alias("event_id"), m.getItem("event_type").alias("event_type"),
                         "stream", "partition_key", "sequence_number", "shard_id", "arrival_ts",
                         "ingested_at", "batch_id", "ingest_date")


def silver_frame(valid: DataFrame, masters, cfg: Config) -> DataFrame:
    """Válidos -> Silver. `dropDuplicates(event_id)` quita los duplicados dentro del lote; los de lotes
    anteriores los descarta el MERGE (ver `process_batch`)."""
    typed = typed_events(valid, cfg.late_threshold_s).dropDuplicates(["event_id"])
    return enrich(typed, masters.products, masters.customers, masters.dt)


def process_batch(spark, batch_df: DataFrame, batch_id: int, cfg: Config, holder: MastersHolder,
                  qid: str = "local") -> dict:
    """Un micro-batch de Q1. Es idempotente: reintentarlo no cambia ninguna tabla.

    - Bronze y cuarentena: append con `txnAppId`/`txnVersion = batch_id` (Delta descarta la segunda escritura).
    - Silver: MERGE solo de inserción por (event_date, event_id); lo ya insertado no se vuelve a insertar.
    """
    holder.refresh_if_due()
    masters = holder.get()
    ingested = now_lit()
    df = (batch_df.withColumn("ingested_at", ingested)
                  .withColumn("batch_id", F.lit(int(batch_id)).cast("long"))
                  .withColumn("ingest_date", F.to_date(F.from_utc_timestamp(F.col("ingested_at"), BOGOTA))))
    # persist: el lote se usa 3 veces (Bronze, cuarentena, Silver). Sin esto Spark volvería a leer la fuente
    # (en Kinesis serían más GetRecords) y las columnas no deterministas (hora de llegada) cambiarían.
    df.persist()
    parsed = None
    stats: dict = {"batch_id": int(batch_id)}
    try:
        with _timed(stats, "count"):
            stats["received"] = df.count()
        if stats["received"] == 0:
            return stats
        # Se materializa la validación una vez. Además de ahorrar re-evaluar el JSON tres veces, acorta el plan:
        # sin esto, Catalyst incrusta las ~60 reglas de cuarentena en el filtro de Silver y optimizar el plan
        # tardaba ~35 s por micro-batch en la imagen de Glue (medido); con el lote en caché, ~2 s.
        with _timed(stats, "parse"):
            parsed = parse_events(df).persist()
            parsed.count()
        valid, rejected = split_valid(parsed)
        w = cfg.write_files  # coalesce: pocos archivos por lote (los archivos pequeños penalizan las lecturas)

        if cfg.source != "bronze":  # al reprocesar desde Bronze no se vuelve a escribir en Bronze
            with _timed(stats, "bronze"):
                bronze = bronze_frame(parsed).coalesce(w)
                append_idempotent(bronze, cfg.bronze, f"tiendacol-bronze-{qid}", batch_id)
                stats["bronze"] = stats["received"]

            with _timed(stats, "quarantine"):
                quarantine = to_quarantine(rejected).coalesce(w)
                stats["quarantine"] = quarantine.count()
                if stats["quarantine"]:
                    append_idempotent(quarantine, cfg.quarantine, f"tiendacol-quarantine-{qid}", batch_id)

        # localCheckpoint(eager): materializa el lote de Silver y corta su linaje, de modo que las pasadas del
        # MERGE (buscar archivos tocados, escribir) no recalculan validación ni enriquecimiento.
        with _timed(stats, "silver_prepare"):
            silver = silver_frame(valid, masters, cfg).localCheckpoint(eager=True)
            stats["silver_candidates"] = silver.count()
        if stats["silver_candidates"]:
            # Un duplicado es byte a byte igual al original: misma event_date y mismo event_id. La condición
            # con event_date es exacta contra TODA la tabla.
            # Poda de particiones: Delta descarta archivos del destino solo con los predicados del ON que
            # tocan únicamente a `t`; la igualdad `t.event_date = s.event_date` no le sirve para eso. Por eso
            # se agregan como literales las fechas presentes en el lote (pocas: una o dos por micro-batch).
            dates = sorted({r[0] for r in silver.select("event_date").distinct().collect()})
            in_dates = ", ".join(f"DATE'{d.isoformat()}'" for d in dates)
            on = f"t.event_date IN ({in_dates}) AND t.event_date = s.event_date AND t.event_id = s.event_id"
            with _timed(stats, "silver_merge"):
                res = merge(silver.coalesce(w), cfg.silver, on)
            stats["silver_inserted"] = res.get("num_inserted_rows")
    finally:
        if parsed is not None:
            parsed.unpersist()
        df.unpersist()
    log.info("Q1 lote %s: %s", batch_id, stats)
    return stats


def start_ingesta(spark, cfg: Config):
    """Envoltorio delgado: la lógica está en `process_batch`."""
    holder = get_holder(spark, cfg)
    ck = cfg.checkpoint("ingesta")
    ids: dict = {}

    def run(batch_df, batch_id):
        qid = ids.setdefault("qid", query_id(spark, ck))
        process_batch(spark, batch_df, batch_id, cfg, holder, qid)

    return (read_source(spark, cfg).writeStream.queryName("ingesta")
            .foreachBatch(run).option("checkpointLocation", ck)
            .trigger(**trigger_kwargs(cfg, cfg.ingest_trigger_s)).start())
