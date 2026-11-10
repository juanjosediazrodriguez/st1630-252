"""Q5 · Ventas por minuto y categoría (RF-09) -> `gold/agg_sales_minute`.

Por cada orden se emite una fila "total" y una por cada categoría distinta de sus líneas. Después se agrupa por
ventana de 1 minuto: en modo *update* cada micro-batch actualiza el minuto en curso sin esperar a que cierre.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

from .config import Config
from .deltaio import merge, read_silver_stream, trigger_kwargs
from .masters import get_holder
from .util import now_lit

log = logging.getLogger("tiendacol.ventas_minuto")

UNKNOWN_CATEGORY = -1   # líneas cuyo producto no está en el maestro: no se mezclan con la fila total (NULL)


def order_rows(silver: DataFrame) -> DataFrame:
    """Función pura. Una fila por (orden, categoría) más una fila total (`category_id` NULL) por orden.
    El valor es la suma de `line_value` de las líneas de esa categoría; las categorías suman exactamente el total."""
    cat = f"coalesce(l.category_id, {UNKNOWN_CATEGORY})"
    per_cat = F.expr(
        f"transform(array_distinct(transform(lines, l -> {cat})), c -> named_struct('category_id', c, "
        f"'value', aggregate(filter(lines, l -> {cat} = c), cast(0 as double), (acc, l) -> acc + l.line_value)))")
    total = F.expr("aggregate(lines, cast(0 as double), (acc, l) -> acc + l.line_value)")
    all_rows = F.concat(F.array(F.struct(F.lit(None).cast("int").alias("category_id"), total.alias("value"))), per_cat)
    return (silver.where(F.col("event_type") == "order_created")
                  .select("event_ts", F.explode(all_rows).alias("r"))
                  .select("event_ts", F.col("r.category_id").alias("category_id"), F.col("r.value").alias("value")))


def minute_agg(silver: DataFrame, cfg: Config) -> DataFrame:
    """Ventas por minuto: watermark de 1 min y ventana de 1 min por categoría (y total)."""
    return (order_rows(silver).withWatermark("event_ts", cfg.sales_watermark)
            .groupBy(F.window("event_ts", "1 minute"), "category_id")
            .agg(F.count(F.lit(1)).alias("orders"), F.sum("value").alias("sales_cop"))
            .select(F.col("window.start").alias("minute_start"), F.col("window.end").alias("minute_end"),
                    "category_id", "orders", "sales_cop"))


def sales_rows(agg: DataFrame, categories: DataFrame) -> DataFrame:
    """Agrega el nombre de la categoría (*broadcast join*) y `updated_at`. La fila total se llama `Total`."""
    cats = categories.select(F.col("category_id").alias("_cid"), "category")
    return (agg.join(F.broadcast(cats), agg["category_id"] == F.col("_cid"), "left")
               .select("minute_start", "minute_end", "category_id",
                       F.when(F.col("category_id").isNull(), F.lit("Total"))
                        .otherwise(F.coalesce("category", F.lit("Sin categoría"))).alias("category"),
                       F.col("orders").cast("long").alias("orders"), F.col("sales_cop").cast("double").alias("sales_cop"),
                       now_lit().alias("updated_at")))


def process_batch(spark, batch_df: DataFrame, batch_id: int, cfg: Config) -> int:
    rows = sales_rows(batch_df, get_holder(spark, cfg).get().categories).localCheckpoint(eager=True)
    n = rows.count()
    if n:
        # `<=>` es la igualdad que trata NULL = NULL: sin ella la fila total (category_id NULL) nunca coincidiría
        # y se insertaría una nueva en cada lote. SET (no "+="): el valor es el acumulado del state store,
        # así que reintentar el lote deja el mismo resultado (idempotencia).
        merge(rows, cfg.agg_sales_minute, "t.minute_start = s.minute_start AND t.category_id <=> s.category_id",
              update_set={"minute_end": "s.minute_end", "category": "s.category", "orders": "s.orders",
                          "sales_cop": "s.sales_cop", "updated_at": "s.updated_at"})
    log.info("Q5 lote %s: %s filas de minuto", batch_id, n)
    return n


def start_ventas_minuto(spark, cfg: Config):
    return (minute_agg(read_silver_stream(spark, cfg), cfg).writeStream.queryName("ventas_minuto")
            .outputMode("update")
            .foreachBatch(lambda df, bid: process_batch(spark, df, bid, cfg))
            .option("checkpointLocation", cfg.checkpoint("ventas_minuto"))
            .trigger(**trigger_kwargs(cfg, cfg.downstream_trigger_s)).start())
