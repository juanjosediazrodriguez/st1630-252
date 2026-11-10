"""Q4 · Carrito abandonado (RF-08): carrito >= COP 500.000, 30 min sin actividad de carrito y sin compra.

Una *session window* de 30 min por usuario agrupa sus eventos de carrito. Con modo *append* la sesión se emite
una sola vez, cuando el watermark rebasa su fin (último evento de carrito + 30 min).
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

from .config import Config
from .deltaio import read_silver_stream, trigger_kwargs
from .sqs import SqsPublisher, make_publisher
from .util import BOGOTA, iso_col, now_lit

log = logging.getLogger("tiendacol.carritos")

CART_TYPES = ["add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
WATERMARK_KEY = "__watermark__"


def cart_signals(silver: DataFrame) -> DataFrame:
    """Función pura. Cada evento de Silver queda con:
    - `user_key`: el usuario si es un evento de carrito de un usuario identificado; si no, `__watermark__`.
    - `cart_delta`: aporte al valor del carrito (+cantidad x precio al agregar, - al quitar; si el evento de
      quitar no trae precio se usa `list_price` del maestro). 0 en el resto.

    NO se filtra nada. Si los eventos que no son de carrito (vistas de página, órdenes, stock, anónimos) se
    descartaran antes de agregar, Spark empujaría el filtro por debajo de `withWatermark` y esos eventos ya no
    moverían el reloj: sin latido de tráfico el watermark se detendría y la sesión nunca cerraría (E1, ADR-2).
    Agrupados bajo la clave `__watermark__` siguen avanzando el reloj y se descartan a la salida.
    """
    et = F.col("event_type")
    is_cart = et.isin(*CART_TYPES) & F.col("is_identified")
    price = F.coalesce("unit_price", "list_price")
    delta = (F.when(et == "add_to_cart", F.col("quantity") * price)
              .when(et == "remove_from_cart", -F.col("quantity") * price)
              .otherwise(F.lit(0.0)))
    return silver.select(
        "event_ts",
        F.when(is_cart, F.col("user_id")).otherwise(F.lit(WATERMARK_KEY)).alias("user_key"),
        F.when(is_cart, F.coalesce(delta, F.lit(0.0))).otherwise(F.lit(0.0)).alias("cart_delta"),
        F.when(is_cart & (et == "purchase"), 1).otherwise(0).alias("is_purchase"),
        F.when(is_cart, 1).otherwise(0).alias("is_cart_event"),
        F.when(is_cart & (et == "add_to_cart"), F.col("product_id")).alias("added_product"))


def abandoned_sessions(silver: DataFrame, cfg: Config) -> DataFrame:
    """Sesiones de carrito cerradas, con el valor y si hubo compra. Sirve en lote y en streaming."""
    gap = f"{cfg.cart_gap_min} minutes"
    sessions = (cart_signals(silver).withWatermark("event_ts", cfg.cart_watermark)
                .groupBy("user_key", F.session_window("event_ts", gap))
                .agg(F.sum("cart_delta").alias("cart_value"), F.sum("is_purchase").alias("purchases"),
                     F.sum("is_cart_event").alias("events_count"), F.max("event_ts").alias("last_event_ts"),
                     F.collect_set("added_product").alias("product_ids")))
    # La clave `__watermark__` se descarta con `events_count > 0` (un agregado), NO con `user_key != '__watermark__'`:
    # un filtro sobre la clave de agrupación se empuja por debajo del agregado y de `withWatermark`, y entonces los
    # latidos dejan de mover el reloj (comprobado: el watermark solo avanzaba con los eventos de carrito).
    return (sessions.where((F.col("events_count") > 0) & (F.col("purchases") == 0)
                           & (F.col("cart_value") >= cfg.cart_min_value))
                    .select(F.col("user_key").alias("user_id"), "cart_value",
                            F.col("session_window.start").alias("session_start"), "last_event_ts",
                            F.col("session_window.end").alias("abandoned_at"), "events_count", "product_ids"))


def to_messages(abandoned: DataFrame) -> list[dict]:
    """Sesiones abandonadas -> mensajes del contrato (DISENO §4.2). El `message_id` es determinista."""
    rows = abandoned.select(
        "user_id", "cart_value", iso_col("session_start").alias("session_start"),
        iso_col("last_event_ts").alias("last_event_ts"), iso_col("abandoned_at").alias("abandoned_at"),
        F.date_format(F.from_utc_timestamp("abandoned_at", BOGOTA), "yyyy-MM-dd").alias("abandoned_date"),
        "events_count", F.array_sort("product_ids").alias("product_ids"),
        iso_col(now_lit()).alias("detected_at")).orderBy("abandoned_at", "user_id").collect()
    return [{"schema_version": "1.0", "decision_type": "abandoned_cart", "message_id": f"{r.user_id}|{r.session_start}",
             "user_id": r.user_id, "cart_value": float(r.cart_value), "currency": "COP",
             "session_start": r.session_start, "last_event_ts": r.last_event_ts, "abandoned_at": r.abandoned_at,
             "abandoned_date": r.abandoned_date, "events_count": int(r.events_count),
             "product_ids": list(r.product_ids), "detected_at": r.detected_at} for r in rows]


def process_batch(spark, batch_df: DataFrame, batch_id: int, cfg: Config, publisher: SqsPublisher) -> int:
    msgs = to_messages(batch_df)
    if msgs:
        # Si publicar falla, la excepción tumba el micro-batch y Spark lo reintenta (at-least-once);
        # la Lambda de P4 hace upsert por user_id, así que un repetido no duplica.
        publisher.publish("carts", msgs)
    log.info("Q4 lote %s: %s carritos abandonados", batch_id, len(msgs))
    return len(msgs)


def start_carritos(spark, cfg: Config):
    publisher = make_publisher(cfg)
    return (abandoned_sessions(read_silver_stream(spark, cfg), cfg).writeStream.queryName("carritos")
            .outputMode("append")
            .foreachBatch(lambda df, bid: process_batch(spark, df, bid, cfg, publisher))
            .option("checkpointLocation", cfg.checkpoint("carritos"))
            .trigger(**trigger_kwargs(cfg, cfg.downstream_trigger_s)).start())
