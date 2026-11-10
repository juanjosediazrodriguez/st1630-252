"""Q2 · Stock actual por producto y bodega (RF-03).

El acumulado de `delta` por (producto, bodega) vive en el *state store* de Spark, respaldado por el checkpoint.
No necesita watermark: el estado está acotado por el catálogo (productos x bodegas) y no crece con el tiempo.
`foreachBatch` hace MERGE con SET (no con incrementos), así que reintentar el lote es idempotente.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

from .config import Config
from .deltaio import merge, read_silver_stream, trigger_kwargs
from .masters import get_holder
from .util import now_lit

log = logging.getLogger("tiendacol.stock")


def aggregate_stock(df: DataFrame) -> DataFrame:
    """Función pura (sirve en lote y en streaming): acumulado de `delta` y último evento por clave.
    En streaming con modo *update* emite solo las claves que cambiaron en el micro-batch."""
    return (df.where(F.col("event_type") == "stock_change")
              .groupBy("product_id", "warehouse_id")
              .agg(F.sum("delta").cast("long").alias("cumulative_delta"),
                   F.max("event_ts").alias("last_event_ts")))


def stock_rows(updates: DataFrame, inventory: DataFrame, masters_dt: str) -> DataFrame:
    """Une el acumulado con el inventario base del maestro (*broadcast join*) y arma las filas de `stock_current`.
    Un par (producto, bodega) que no está en el maestro arranca con base 0."""
    inv = inventory.select("product_id", "warehouse_id", "base_stock")
    return (updates.join(F.broadcast(inv), ["product_id", "warehouse_id"], "left")
                   .select("product_id", "warehouse_id",
                           F.coalesce("base_stock", F.lit(0)).cast("int").alias("base_stock"),
                           F.col("cumulative_delta").cast("long").alias("cumulative_delta"),
                           (F.coalesce("base_stock", F.lit(0)) + F.col("cumulative_delta")).cast("long").alias("stock"),
                           F.lit(masters_dt).alias("masters_dt"),
                           "last_event_ts", now_lit().alias("updated_at")))


def seed_stock(spark, cfg: Config) -> None:
    """Siembra `stock_current` con el inventario del maestro (acumulado 0) para los pares que falten.
    Sin esto, un producto sin movimientos no aparecería y el stock total por producto saldría incompleto.
    `base_stock` se escribe una sola vez y no se re-basa con maestros posteriores (limitación conocida)."""
    m = get_holder(spark, cfg).get()
    seed = m.inventory.select(
        "product_id", "warehouse_id", F.col("base_stock").cast("int").alias("base_stock"),
        F.lit(0).cast("long").alias("cumulative_delta"), F.col("base_stock").cast("long").alias("stock"),
        F.lit(m.dt).alias("masters_dt"), F.lit(None).cast("timestamp").alias("last_event_ts"),
        now_lit().alias("updated_at"))
    merge(seed, cfg.stock_current, "t.product_id = s.product_id AND t.warehouse_id = s.warehouse_id")


def process_batch(spark, batch_df: DataFrame, batch_id: int, cfg: Config) -> int:
    m = get_holder(spark, cfg).get()
    rows = stock_rows(batch_df, m.inventory, m.dt).localCheckpoint(eager=True)
    n = rows.count()
    if n:
        # SET (no "+="): el acumulado viene completo del state store, por eso un reintento deja el mismo resultado.
        merge(rows, cfg.stock_current, "t.product_id = s.product_id AND t.warehouse_id = s.warehouse_id",
              update_set={"cumulative_delta": "s.cumulative_delta", "stock": "t.base_stock + s.cumulative_delta",
                          "last_event_ts": "s.last_event_ts", "updated_at": "s.updated_at"})
    log.info("Q2 lote %s: %s claves actualizadas", batch_id, n)
    return n


def start_stock(spark, cfg: Config):
    agg = aggregate_stock(read_silver_stream(spark, cfg))
    return (agg.writeStream.queryName("stock").outputMode("update")
            .foreachBatch(lambda df, bid: process_batch(spark, df, bid, cfg))
            .option("checkpointLocation", cfg.checkpoint("stock"))
            .trigger(**trigger_kwargs(cfg, cfg.downstream_trigger_s)).start())
