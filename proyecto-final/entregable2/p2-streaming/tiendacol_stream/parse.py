"""Validación y tipado de eventos (DISENO §3.2 y §3.3). Todas son funciones puras DataFrame -> DataFrame.

Estrategia: el payload se lee con `from_json` a un `map<string,string>`. Así
  - un JSON inválido o truncado da `null` (no hay mapa) -> `invalid_json`;
  - cualquier valor escalar llega como texto y se castea después; un cast que da `null` revela un tipo
    erróneo (`wrong_type`) sin perder el resto del registro (con un esquema tipado, Spark anularía la fila
    entera y no se sabría qué campo falló);
  - los objetos anidados (`lines`, `payment`) llegan como texto JSON y se leen con un segundo `from_json`.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (ArrayType, MapType, StringType, StructField, StructType)

from .tables import SILVER_SCHEMA
from .util import BOGOTA

CLICK_TYPES = ["page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
ORDER_TYPES = ["order_created", "order_status_changed"]
STOCK_TYPES = ["stock_change"]
VALID_TYPES = CLICK_TYPES + ORDER_TYPES + STOCK_TYPES

# Campos requeridos por tipo (DISENO §3.2, regla 5). `lines` se trata aparte (no vacío).
REQUIRED = {
    **{t: ["session_id"] for t in CLICK_TYPES},
    "add_to_cart": ["session_id", "product_id", "quantity", "unit_price"],
    "remove_from_cart": ["session_id", "product_id", "quantity"],
    "purchase": ["session_id", "order_id"],
    "order_created": ["order_id", "total"],
    "order_status_changed": ["order_id", "status"],
    "stock_change": ["product_id", "warehouse_id", "delta", "reason"],
}
NUMERIC = {"quantity": "int", "unit_price": "double", "cart_value": "double", "delta": "int", "total": "double"}

_MAP = MapType(StringType(), StringType())
_S = StringType()
LINES_RAW = ArrayType(StructType([StructField(n, _S) for n in ("product_id", "quantity", "unit_price", "discount")]))
PAYMENT_RAW = StructType([StructField(n, _S) for n in ("method", "amount", "status")])

# ISO-8601 con fecha, hora y (opcional) fracción y zona. Más estricto que el cast de Spark, que acepta fechas sueltas.
_ISO_RE = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:?\d{2})?$"


def _miss(c: Column) -> Column:
    return c.isNull() | (F.trim(c) == "")


def stream_of(event_type: Column) -> Column:
    return (F.when(event_type.isin(*CLICK_TYPES), "clickstream")
             .when(event_type.isin(*ORDER_TYPES), "orders")
             .when(event_type.isin(*STOCK_TYPES), "stock_changes"))


def _first(conds: list[tuple[Column, str]]) -> Column:
    """Primera regla que falla (el orden de la lista es el orden del contrato). Una condición nula no falla."""
    return F.coalesce(*[F.when(c, F.lit(r)) for c, r in conds])


def parse_events(raw_df: DataFrame) -> DataFrame:
    """Agrega `_m` (mapa del JSON, o null) y `reject_reason` (null = válido; si no, la primera regla incumplida)."""
    df = raw_df.withColumn("_m", F.from_json("raw", _MAP))
    m = F.col("_m")
    g = lambda k: m.getItem(k)  # noqa: E731
    et = g("event_type")
    lines = F.from_json(g("lines"), LINES_RAW)

    conds: list[tuple[Column, str]] = [(m.isNull(), "invalid_json")]                          # 1
    conds += [(_miss(g(f)), f"missing_field:{f}") for f in ("event_id", "event_type", "event_ts")]  # 2
    conds.append((~et.isin(*VALID_TYPES), "unknown_event_type"))                              # 3
    conds.append((~(g("event_ts").rlike(_ISO_RE) & g("event_ts").cast("timestamp").isNotNull()), "wrong_type:event_ts"))  # 4
    conds += [(g(f).isNotNull() & g(f).cast(t).isNull(), f"wrong_type:{f}") for f, t in NUMERIC.items()]
    bad_line = F.exists(lines, lambda l: _miss(l["product_id"]) | l["quantity"].cast("int").isNull()
                        | l["unit_price"].cast("double").isNull()
                        | (l["discount"].isNotNull() & l["discount"].cast("double").isNull()))
    conds.append(((et == "order_created") & g("lines").isNotNull() & (lines.isNull() | F.coalesce(bad_line, F.lit(False))),
                  "wrong_type:lines"))
    for t, req in REQUIRED.items():                                                           # 5
        for f in req:
            conds.append(((et == t) & _miss(g(f)), f"missing_field:{f}"))
    conds.append(((et == "order_created") & (_miss(g("lines")) | (F.size(lines) == 0)), "missing_field:lines"))
    conds.append((F.coalesce(g("schema_version"), F.lit("1.0")) != "1.0", "unsupported_schema_version"))  # 6
    return df.withColumn("reject_reason", _first(conds))


def split_valid(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(válidos, rechazados) a partir de `parse_events`."""
    return parsed.where(F.col("reject_reason").isNull()), parsed.where(F.col("reject_reason").isNotNull())


def to_quarantine(rejected: DataFrame) -> DataFrame:
    m = F.col("_m")
    return rejected.select(
        "raw", m.getItem("event_id").alias("event_id"), m.getItem("event_type").alias("event_type"),
        "stream", "sequence_number", F.col("reject_reason").alias("reason"), "ingested_at", "batch_id", "ingest_date")


def typed_events(valid: DataFrame, late_seconds: int = 60) -> DataFrame:
    """Válidos -> columnas de Silver SIN enriquecer (tipos del contrato). Espera `ingested_at` y `batch_id`.

    Las columnas de enriquecimiento (`city` del maestro, categoría, ...) se llenan en `enrich.py`; aquí la
    ciudad del evento queda en `event_city`/`event_department` para que el enriquecimiento decida la fuente.
    """
    m = F.col("_m")
    g = lambda k: m.getItem(k)  # noqa: E731
    et = g("event_type")
    uid = F.when(_miss(g("user_id")), F.lit(None).cast("string")).otherwise(g("user_id"))
    ts = g("event_ts").cast("timestamp")
    pay = F.from_json(g("payment"), PAYMENT_RAW)
    lines = F.from_json(g("lines"), LINES_RAW)
    typed_lines = F.transform(lines, lambda l: F.struct(
        l["product_id"].alias("product_id"), l["quantity"].cast("int").alias("quantity"),
        l["unit_price"].cast("double").alias("unit_price"),
        F.coalesce(l["discount"].cast("double"), F.lit(0.0)).alias("discount")))
    is_click = et.isin(*CLICK_TYPES)
    return valid.select(
        g("event_id").alias("event_id"), et.alias("event_type"), ts.alias("event_ts"),
        F.to_date(F.from_utc_timestamp(ts, BOGOTA)).alias("event_date"),
        F.coalesce(g("schema_version"), F.lit("1.0")).alias("schema_version"),
        stream_of(et).alias("stream"),
        g("session_id").alias("session_id"), uid.alias("user_id"), uid.isNotNull().alias("is_identified"),
        g("device").alias("device"),
        F.when(is_click | et.isin(*STOCK_TYPES), g("product_id")).alias("product_id"),
        g("query").alias("query"), g("quantity").cast("int").alias("quantity"),
        g("unit_price").cast("double").alias("unit_price"), g("cart_value").cast("double").alias("cart_value"),
        g("order_id").alias("order_id"), g("status").alias("status"), g("previous_status").alias("previous_status"),
        g("total").cast("double").alias("total"), pay["method"].alias("payment_method"),
        pay["amount"].cast("double").alias("payment_amount"), pay["status"].alias("payment_status"),
        typed_lines.alias("lines_raw"),
        g("warehouse_id").alias("warehouse_id"), g("delta").cast("int").alias("delta"), g("reason").alias("reason_stock"),
        g("city").alias("event_city"), g("department").alias("event_department"),
        "arrival_ts", "ingested_at", "sequence_number", "batch_id",
        ((F.col("arrival_ts").cast("double") - ts.cast("double")) > F.lit(float(late_seconds))).alias("is_late"))


def silver_select(df: DataFrame) -> DataFrame:
    """Selecciona y castea exactamente las columnas de Silver, en el orden de la tabla."""
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields])
