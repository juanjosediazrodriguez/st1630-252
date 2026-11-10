"""Esquemas Delta de las tablas de Gold (P3) y utilidades de MERGE SQL compatibles con AWS Glue 4.0."""
from __future__ import annotations

import hashlib
import uuid

from .config import GoldConfig


def surrogate_key(*parts) -> str:
    """Genera una llave subrogada determinista (16 hex chars) a partir de los componentes de negocio."""
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def delta_ident(path: str) -> str:
    """Identificador SQL de tabla Delta por ruta: delta.`ruta` (no requiere módulo Python delta)."""
    return "delta.`" + path.replace("`", "") + "`"


def view_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _fs(spark, path: str):
    jvm = spark._jvm
    uri = jvm.java.net.URI(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration()), jvm.org.apache.hadoop.fs.Path(path)


def path_exists(spark, path: str) -> bool:
    fs, p = _fs(spark, path)
    return bool(fs.exists(p))


def is_delta(spark, path: str) -> bool:
    return path_exists(spark, path.rstrip("/") + "/_delta_log")


def glob_paths(spark, pattern: str) -> list[str]:
    fs, p = _fs(spark, pattern)
    st = fs.globStatus(p)
    return sorted(s.getPath().toString() for s in st) if st is not None else []


def read_text(spark, path: str) -> str:
    return "\n".join(r.value for r in spark.read.text(path).collect())


def get_gold_schemas():
    """Devuelve los StructType de las tablas de Gold (importa pyspark perezosamente)."""
    from pyspark.sql.types import (
        BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
    )
    S, I, L, D, T, B, DT = (
        StringType(), IntegerType(), LongType(), DoubleType(), TimestampType(), BooleanType(), DateType()
    )

    dim_product = StructType([
        StructField("product_key", S),
        StructField("product_id", S),
        StructField("name", S),
        StructField("category_id", I),
        StructField("category", S),
        StructField("seller_id", S),
        StructField("price", D),
        StructField("attributes", S),
        StructField("valid_from", S),
        StructField("valid_to", S),
        StructField("is_current", B),
    ])

    dim_seller = StructType([
        StructField("seller_key", S),
        StructField("seller_id", S),
        StructField("seller_name", S),
        StructField("city", S),
        StructField("department", S),
    ])

    dim_customer = StructType([
        StructField("customer_key", S),
        StructField("customer_id", S),
        StructField("city", S),
        StructField("department", S),
        StructField("created_at", S),
    ])

    dim_location = StructType([
        StructField("location_key", S),
        StructField("city", S),
        StructField("department", S),
    ])

    dim_device = StructType([
        StructField("device_key", S),
        StructField("device_name", S),
    ])

    dim_datetime = StructType([
        StructField("datetime_key", S),
        StructField("minute_ts", T),
        StructField("date", S),
        StructField("hour", S),
        StructField("year", I),
        StructField("month", I),
        StructField("day", I),
        StructField("hour_of_day", I),
        StructField("minute", I),
    ])

    fact_funnel_event = StructType([
        StructField("funnel_event_id", S),
        StructField("event_id", S),
        StructField("session_id", S),
        StructField("user_id", S),
        StructField("stage", S),
        StructField("product_key", S),
        StructField("product_id", S),
        StructField("category_id", I),
        StructField("device_key", S),
        StructField("device", S),
        StructField("location_key", S),
        StructField("city", S),
        StructField("department", S),
        StructField("datetime_key", S),
        StructField("hour", S),
        StructField("event_ts", T),
        StructField("event_date", DT),
        StructField("order_id", S),
    ])

    fact_sales = StructType([
        StructField("sale_line_id", S),
        StructField("event_id", S),
        StructField("order_id", S),
        StructField("line_number", I),
        StructField("product_key", S),
        StructField("product_id", S),
        StructField("category_id", I),
        StructField("seller_key", S),
        StructField("seller_id", S),
        StructField("customer_key", S),
        StructField("user_id", S),
        StructField("location_key", S),
        StructField("city", S),
        StructField("department", S),
        StructField("datetime_key", S),
        StructField("date", S),
        StructField("event_ts", T),
        StructField("event_date", DT),
        StructField("payment_method", S),
        StructField("order_status", S),
        StructField("quantity", I),
        StructField("unit_price", D),
        StructField("discount", D),
        StructField("gross_value", D),
        StructField("net_value", D),
    ])

    agg_product_affinity = StructType([
        StructField("product_id", S),
        StructField("related_product_id", S),
        StructField("co_orders", L),
        StructField("rank", I),
    ])

    return {
        "dim_product": ("gold/dim_product", dim_product, None),
        "dim_seller": ("gold/dim_seller", dim_seller, None),
        "dim_customer": ("gold/dim_customer", dim_customer, None),
        "dim_location": ("gold/dim_location", dim_location, None),
        "dim_device": ("gold/dim_device", dim_device, None),
        "dim_datetime": ("gold/dim_datetime", dim_datetime, None),
        "fact_funnel_event": ("gold/fact_funnel_event", fact_funnel_event, "event_date"),
        "fact_sales": ("gold/fact_sales", fact_sales, "event_date"),
        "agg_product_affinity": ("gold/agg_product_affinity", agg_product_affinity, None),
    }


def ensure_gold_tables(spark, cfg: GoldConfig) -> list[str]:
    """Crea las tablas Delta de Gold que aún no existan en el lago."""
    created = []
    for name, (rel, schema, part) in get_gold_schemas().items():
        path = cfg.path(rel)
        if is_delta(spark, path):
            continue
        w = spark.createDataFrame([], schema).write.format("delta").mode("append")
        if part:
            w = w.partitionBy(part)
        w.save(path)
        created.append(name)
    return created


def merge_delta_sql(spark, source_df, path: str, on_expr: str, update_set: dict[str, str] | None = None,
                    insert: bool = True, write_files: int = 1) -> None:
    """Ejecuta MERGE INTO delta.`path` por SQL (compatible con AWS Glue 4.0 sin librería Python delta)."""
    if write_files and write_files > 0:
        source_df = source_df.coalesce(write_files)
    view = view_name("gold_src")
    source_df.createOrReplaceTempView(view)
    sql = f"MERGE INTO {delta_ident(path)} AS t USING {view} AS s ON {on_expr}"
    if update_set:
        sql += " WHEN MATCHED THEN UPDATE SET " + ", ".join(f"t.{k} = {v}" for k, v in update_set.items())
    if insert:
        sql += " WHEN NOT MATCHED THEN INSERT *"
    try:
        spark.sql(sql)
    finally:
        spark.catalog.dropTempView(view)
