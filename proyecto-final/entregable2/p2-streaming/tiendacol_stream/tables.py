"""Esquemas de las tablas Delta de P2 y su creación (vacías) al arrancar.

Leer como stream una ruta Delta que todavía no existe falla; por eso el job crea todas las tablas
antes de iniciar las consultas. Las particiones (`ingest_date`, `event_date`) permiten podar archivos:
las consultas de un día no leen el resto del lago.
"""
from __future__ import annotations

from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType, TimestampType)

from .config import Config
from .util import is_delta

S, I, L, D, T, B, DT = (StringType(), IntegerType(), LongType(), DoubleType(), TimestampType(),
                        BooleanType(), DateType())

BRONZE_SCHEMA = StructType([
    StructField("raw", S), StructField("event_id", S), StructField("event_type", S), StructField("stream", S),
    StructField("partition_key", S), StructField("sequence_number", S), StructField("shard_id", S),
    StructField("arrival_ts", T), StructField("ingested_at", T), StructField("batch_id", L),
    StructField("ingest_date", DT)])

QUARANTINE_SCHEMA = StructType([
    StructField("raw", S), StructField("event_id", S), StructField("event_type", S), StructField("stream", S),
    StructField("sequence_number", S), StructField("reason", S), StructField("ingested_at", T),
    StructField("batch_id", L), StructField("ingest_date", DT)])

LINE_TYPE = StructType([
    StructField("product_id", S), StructField("quantity", I), StructField("unit_price", D),
    StructField("discount", D), StructField("category_id", I), StructField("seller_id", S),
    StructField("line_value", D)])

SILVER_SCHEMA = StructType([
    # envoltorio
    StructField("event_id", S), StructField("event_type", S), StructField("event_ts", T),
    StructField("event_date", DT), StructField("schema_version", S), StructField("stream", S),
    # clickstream
    StructField("session_id", S), StructField("user_id", S), StructField("is_identified", B),
    StructField("device", S), StructField("product_id", S), StructField("query", S),
    StructField("quantity", I), StructField("unit_price", D), StructField("cart_value", D),
    # órdenes
    StructField("order_id", S), StructField("status", S), StructField("previous_status", S),
    StructField("total", D), StructField("payment_method", S), StructField("payment_amount", D),
    StructField("payment_status", S), StructField("lines", ArrayType(LINE_TYPE)),
    # stock
    StructField("warehouse_id", S), StructField("delta", I), StructField("reason", S),
    # enriquecimiento (RF-06)
    StructField("city", S), StructField("department", S), StructField("geo_source", S),
    StructField("category_id", I), StructField("category", S), StructField("seller_id", S),
    StructField("product_name", S), StructField("list_price", D), StructField("masters_dt", S),
    # linaje
    StructField("arrival_ts", T), StructField("ingested_at", T), StructField("sequence_number", S),
    StructField("batch_id", L), StructField("is_late", B)])

STOCK_SCHEMA = StructType([
    StructField("product_id", S), StructField("warehouse_id", S), StructField("base_stock", I),
    StructField("cumulative_delta", L), StructField("stock", L), StructField("masters_dt", S),
    StructField("last_event_ts", T), StructField("updated_at", T)])

ALERT_LOG_SCHEMA = StructType([
    StructField("alert_id", S), StructField("product_id", S), StructField("product_name", S),
    StructField("category_id", I), StructField("stock", L), StructField("units_15m", L),
    StructField("velocity_per_min", D), StructField("coverage_min", D), StructField("window_start", T),
    StructField("window_end", T), StructField("last_sale_ts", T), StructField("detected_at", T)])

SALES_MINUTE_SCHEMA = StructType([
    StructField("minute_start", T), StructField("minute_end", T), StructField("category_id", I),
    StructField("category", S), StructField("orders", L), StructField("sales_cop", D),
    StructField("updated_at", T)])

# nombre lógico -> (ruta relativa, esquema, columna de partición)
TABLES = {
    "bronze": ("bronze/events", BRONZE_SCHEMA, "ingest_date"),
    "quarantine": ("quarantine/events", QUARANTINE_SCHEMA, "ingest_date"),
    "silver": ("silver/events", SILVER_SCHEMA, "event_date"),
    "stock_current": ("silver/stock_current", STOCK_SCHEMA, None),
    "alert_log": ("silver/alert_log", ALERT_LOG_SCHEMA, None),
    "agg_sales_minute": ("gold/agg_sales_minute", SALES_MINUTE_SCHEMA, None),
}


def ensure_tables(spark, cfg: Config) -> list[str]:
    """Crea las tablas Delta que falten (vacías, con esquema y partición). Devuelve las creadas."""
    created = []
    for name, (rel, schema, part) in TABLES.items():
        path = cfg.path(rel)
        if is_delta(spark, path):
            continue
        w = spark.createDataFrame([], schema).write.format("delta").mode("append")
        if part:
            w = w.partitionBy(part)
        w.save(path)
        created.append(name)
    return created
