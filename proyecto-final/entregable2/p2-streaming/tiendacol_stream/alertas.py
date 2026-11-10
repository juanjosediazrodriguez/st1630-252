"""Q3 · Alerta de agotamiento (RF-07): velocidad de venta de los últimos 15 min contra el stock.

Ventas = `stock_change` con `reason = 'sale'`; unidades = |delta|. Se cuentan desde Silver (ya deduplicado):
si se contaran desde Kinesis, el 1 % de duplicados inflaría la velocidad.
Hay alerta si `velocidad > 0` y `cobertura = stock / velocidad < 60 min`, con `velocidad = unidades_15m / 15`.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window, functions as F

from .config import Config
from .deltaio import merge, read_silver_stream, trigger_kwargs
from .sqs import SqsPublisher, make_publisher
from .util import iso_col, now_lit

log = logging.getLogger("tiendacol.alertas")


def window_units(silver: DataFrame, cfg: Config) -> DataFrame:
    """Unidades vendidas por producto en ventanas deslizantes de `alert_window_min` minutos (paso 1 min).

    El watermark de 1 min acota el estado: una ventana se descarta cuando el reloj de eventos la rebasa.
    Un evento más tardío que eso se ignora en tiempo real (el batch horario de P3 lo recupera).
    `last_sale_ts` es la última venta de la ventana: es la marca contra la que se mide la latencia del RF-07.
    """
    sales = (silver.where((F.col("event_type") == "stock_change") & (F.col("reason") == "sale"))
                   .select("product_id", "product_name", "category_id", "event_ts",
                           F.abs(F.col("delta")).alias("units")))
    return (sales.withWatermark("event_ts", cfg.alert_watermark)
                 .groupBy("product_id",
                          F.window("event_ts", f"{cfg.alert_window_min} minutes", f"{cfg.alert_slide_min} minutes"))
                 .agg(F.sum("units").cast("long").alias("units_15m"),
                      F.max("event_ts").alias("last_sale_ts"),
                      F.first("product_name", ignorenulls=True).alias("product_name"),
                      F.first("category_id", ignorenulls=True).alias("category_id")))


def current_windows(windows: DataFrame, cfg: Config) -> DataFrame:
    """Función pura. De las ventanas que cambiaron en el lote, deja UNA por producto: la que termina en el primer
    minuto exacto posterior a su última venta (`date_trunc('minute', max_sale) + 1 min`). Esa ventana cubre los
    últimos 15 min y se recalcula cada minuto, como pide el E1."""
    w = Window.partitionBy("product_id")
    return (windows.withColumn("_max_sale", F.max("last_sale_ts").over(w))
                   .where(F.col("window.end") == F.date_trunc("minute", F.col("_max_sale")) + F.expr("INTERVAL 1 MINUTE"))
                   .select("product_id", "product_name", "category_id",
                           F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                           "units_15m", "last_sale_ts"))


def with_stock(spark, cands: DataFrame, cfg: Config) -> DataFrame:
    """Agrega `stock` = inventario base (de `silver/stock_current`) + suma de los `delta` hasta la última venta
    de la ventana (de Silver).

    Se calcula con la hora del evento y no con el estado actual de `stock_current` para que el resultado no dependa
    de la carrera entre Q2 y Q3 (corren en paralelo) ni de cuánto se haya adelantado Q2 al reprocesar: la primera
    alerta sale en la venta en que el stock *a esa hora* cruza el umbral. En tiempo real ambos coinciden.
    La lectura se limita a los productos del lote (`isin`), así que es pequeña.
    """
    ids = [r.product_id for r in cands.select("product_id").distinct().collect()]
    base = (spark.read.format("delta").load(cfg.stock_current).where(F.col("product_id").isin(ids))
                 .groupBy("product_id").agg(F.sum("base_stock").cast("long").alias("_base")))
    ev = (spark.read.format("delta").load(cfg.silver)
               .where((F.col("event_type") == "stock_change") & F.col("product_id").isin(ids))
               .select("product_id", F.col("event_ts").alias("_ets"), "delta"))
    moved = (ev.join(cands.select("product_id", "last_sale_ts"), "product_id")
               .where(F.col("_ets") <= F.col("last_sale_ts"))
               .groupBy("product_id").agg(F.sum("delta").cast("long").alias("_moved")))
    return (cands.join(base, "product_id", "left").join(moved, "product_id", "left")
                 .withColumn("stock", F.coalesce("_base", F.lit(0)) + F.coalesce("_moved", F.lit(0)))
                 .drop("_base", "_moved"))


def evaluate_alerts(cands: DataFrame, cfg: Config) -> DataFrame:
    """Función pura. Aplica la regla del RF-07 sobre candidatos con `stock`.

    La condición se escribe con enteros (`stock x 15 < 60 x unidades`) y no con la cobertura en decimales:
    en el límite exacto (64 ventas, stock 256 -> cobertura 60,0) una división en coma flotante podría dar
    59,999... y disparar la alerta una venta antes.
    """
    win, hor = float(cfg.alert_window_min), float(cfg.alert_horizon_min)
    ok = (F.col("units_15m") > 0) & (F.col("stock") * F.lit(win) < F.lit(hor) * F.col("units_15m"))
    return (cands.where(ok)
                 .withColumn("velocity_per_min", F.col("units_15m") / F.lit(win))
                 .withColumn("coverage_min", F.col("stock") * F.lit(win) / F.col("units_15m"))
                 .withColumn("alert_id", F.concat_ws("|", "product_id", iso_col("window_end")))
                 .withColumn("detected_at", now_lit())
                 .select("alert_id", "product_id", "product_name", "category_id", "stock", "units_15m",
                         "velocity_per_min", "coverage_min", "window_start", "window_end", "last_sale_ts", "detected_at"))


def apply_cooldown(alerts: DataFrame, log_df: DataFrame, cfg: Config) -> DataFrame:
    """Función pura. Descarta la alerta si el mismo producto ya tiene otra en `alert_log` en los últimos
    `alert_cooldown_min` minutos *de tiempo de evento*. Medir en tiempo de evento (y no de reloj) hace que
    el reproceso de un día completo se comporte igual que el tiempo real. También cubre el reintento de un
    lote: la alerta ya registrada se descarta, y si el job cayó antes de registrarla, se publica otra vez
    con el mismo `alert_id` (el consumidor la descarta)."""
    a, l = alerts.alias("a"), log_df.select("product_id", F.col("last_sale_ts").alias("_prev")).alias("l")
    seen = ((F.col("a.product_id") == F.col("l.product_id"))
            & (F.col("l._prev") > F.col("a.last_sale_ts") - F.expr(f"INTERVAL {int(cfg.alert_cooldown_min)} MINUTES")))
    return a.join(l, seen, "left_anti")


def to_messages(alerts: DataFrame) -> list[dict]:
    """Alertas -> mensajes del contrato (DISENO §4.1). Los timestamps se formatean en Spark (UTC)."""
    rows = alerts.select(
        "alert_id", "product_id", "product_name", "category_id", "stock", "units_15m", "velocity_per_min",
        "coverage_min", iso_col("window_start").alias("window_start"), iso_col("window_end").alias("window_end"),
        iso_col("last_sale_ts").alias("last_sale_ts"), iso_col("detected_at").alias("detected_at")
    ).orderBy("last_sale_ts").collect()
    return [{"schema_version": "1.0", "decision_type": "stock_depletion_alert", "alert_id": r.alert_id,
             "product_id": r.product_id, "product_name": r.product_name, "category_id": r.category_id,
             "stock": r.stock, "units_15m": r.units_15m, "velocity_per_min": round(r.velocity_per_min, 3),
             "coverage_min": round(r.coverage_min, 1), "window_start": r.window_start, "window_end": r.window_end,
             "last_sale_ts": r.last_sale_ts, "detected_at": r.detected_at} for r in rows]


def process_batch(spark, batch_df: DataFrame, batch_id: int, cfg: Config, publisher: SqsPublisher) -> int:
    cands = current_windows(batch_df, cfg).localCheckpoint(eager=True)
    if cands.count() == 0:
        return 0
    alerts = evaluate_alerts(with_stock(spark, cands, cfg), cfg)
    log_df = spark.read.format("delta").load(cfg.alert_log)
    alerts = apply_cooldown(alerts, log_df, cfg).localCheckpoint(eager=True)
    msgs = to_messages(alerts)
    if msgs:
        # Orden: primero SQS y después alert_log. Si el job cae entre ambos, el reintento publica otra vez el mismo
        # alert_id (at-least-once + consumidor idempotente). Al revés se podría perder la alerta.
        publisher.publish("alerts", msgs)
        merge(alerts, cfg.alert_log, "t.alert_id = s.alert_id")
    log.info("Q3 lote %s: %s alertas", batch_id, len(msgs))
    return len(msgs)


def start_alertas(spark, cfg: Config):
    publisher = make_publisher(cfg)
    return (window_units(read_silver_stream(spark, cfg), cfg).writeStream.queryName("alertas")
            .outputMode("update")
            .foreachBatch(lambda df, bid: process_batch(spark, df, bid, cfg, publisher))
            .option("checkpointLocation", cfg.checkpoint("alertas"))
            .trigger(**trigger_kwargs(cfg, cfg.downstream_trigger_s)).start())
