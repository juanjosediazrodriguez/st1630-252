"""Orquestación del job horario de AWS Glue Batch (P3): lee Silver y Maestros, actualiza Gold en Delta Lake."""
from __future__ import annotations

import json
import logging
import re

import pandas as pd
import numpy as np

from .config import GoldConfig
from .dimensions import apply_scd2_products_df
from .facts import build_gold_star_from_data
from .tables import (
    ensure_gold_tables,
    get_gold_schemas,
    glob_paths,
    is_delta,
    merge_delta_sql,
    read_text,
)

log = logging.getLogger(__name__)
_MANIFEST_RE = re.compile(r"_manifest_dt=(\d{4}-\d{2}-\d{2})\.json$")


def _spark_record(record: dict) -> dict:
    """Pandas vuelve a convertir datetime de Python en Timestamp al crear el DataFrame.

    PySpark con esquema explícito acepta datetime/date/escalares nativos, no Timestamp/NaT
    ni varios escalares numpy. Normalizar justo antes de createDataFrame evita el fallo.
    """
    out = {}
    for key, value in record.items():
        if value is None or value is pd.NaT:
            value = None
        elif isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        elif isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, float) and pd.isna(value):
            value = None
        out[key] = value
    return out


def _load_spark_masters_snapshots(spark, masters_root: str) -> list[tuple[str, dict]]:
    """Lee todos los snapshots de maestros en `masters_root` vía Hadoop/S3 para reconstruir o evolucionar SCD2."""
    manifests = []
    for p in glob_paths(spark, f"{masters_root}/_manifest_dt=*.json"):
        m = _MANIFEST_RE.search(p)
        if m:
            manifests.append((m.group(1), p))
    if not manifests:
        raise RuntimeError(
            f"No se encontró ningún _manifest_dt=*.json en {masters_root}. "
            "Ejecute primero la carga diaria de maestros (P1)."
        )

    snapshots = []
    for day, mpath in sorted(manifests):
        manifest = json.loads(read_text(spark, mpath))
        files = manifest.get("files", {})
        mdata = {}
        for entity in ("products", "customers", "sellers"):
            rel = files.get(entity)
            if not rel:
                continue
            path = f"{masters_root}/{rel}"
            if rel.endswith(".json"):
                raw = spark.read.option("multiLine", True).json(path)
                pdf = raw.toPandas()
                if "attributes" in pdf.columns:
                    pdf["attributes"] = pdf["attributes"].map(
                        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else str(x)
                    )
                mdata[entity] = pdf
            else:
                df = (
                    spark.read.option("header", True)
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("multiLine", True)
                    .csv(path)
                )
                mdata[entity] = df.toPandas()
        snapshots.append((day, mdata))
    return snapshots


def run_job(cfg: GoldConfig, spark) -> dict[str, int]:
    """Ejecuta el batch horario de Gold sobre Spark + Delta Lake:

    1. Asegura que las 9 tablas Delta de Gold existan en `<lake_root>/gold/`.
    2. Lee todos los snapshots de maestros (`landing/masters/_manifest_dt=*.json`) y la
       tabla `dim_product` actual para evolucionar el historial SCD Tipo 2 de manera idempotente.
    3. Lee `silver/events` (filtrando por `process_date` si fue indicado).
    4. Construye dimensiones (`dim_product`, `dim_seller`, `dim_customer`, `dim_location`,
       `dim_device`, `dim_datetime`), tablas de hechos (`fact_sales`, `fact_funnel_event`)
       y afinidad (`agg_product_affinity`).
    5. Hace MERGE INTO por ruta Delta, de forma idempotente, sobre cada tabla de Gold.
    """
    created = ensure_gold_tables(spark, cfg)
    if created:
        log.info("Tablas Delta de Gold inicializadas: %s", created)

    snapshots = _load_spark_masters_snapshots(spark, cfg.masters_root)

    # Si dim_product ya tenía filas en Gold, partir de ese estado para preservar SCD2 previo
    existing_dim_product = None
    if is_delta(spark, cfg.dim_product):
        existing_pdf = spark.read.format("delta").load(cfg.dim_product).toPandas()
        if not existing_pdf.empty:
            existing_dim_product = existing_pdf

    # Leer eventos de Silver escritos por P2
    if is_delta(spark, cfg.silver_events):
        silver_df = spark.read.format("delta").load(cfg.silver_events)
        if cfg.process_date:
            silver_df = silver_df.where(silver_df.event_date == cfg.process_date)
        silver_records = [r.asDict(recursive=True) for r in silver_df.collect()]
    else:
        log.warning("La tabla Silver %s aún no existe; se actualizarán únicamente las dimensiones.", cfg.silver_events)
        silver_records = []

    clicks = [r for r in silver_records if r.get("stream") == "clickstream" or r.get("event_type") in ("page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start", "purchase")]
    orders = [r for r in silver_records if r.get("stream") == "orders" or r.get("event_type") in ("order_created", "order_status_changed")]

    star = build_gold_star_from_data(clicks, orders, snapshots, top_affinity_k=cfg.top_affinity_k)

    # Si ya existía dim_product en Gold, aplicar SCD2 incremental sobre la existente
    if existing_dim_product is not None:
        dim_p = existing_dim_product
        for day, mdata in snapshots:
            if "products" in mdata:
                dim_p = apply_scd2_products_df(dim_p, mdata["products"], day)
        star["dim_product"] = dim_p

    schemas = get_gold_schemas()
    merge_keys = {
        "dim_product": (
            "t.product_key = s.product_key",
            {
                "name": "s.name",
                "category_id": "s.category_id",
                "category": "s.category",
                "seller_id": "s.seller_id",
                "price": "s.price",
                "attributes": "s.attributes",
                "valid_to": "s.valid_to",
                "is_current": "s.is_current",
            },
        ),
        "dim_seller": (
            "t.seller_key = s.seller_key",
            {"seller_name": "s.seller_name", "city": "s.city", "department": "s.department"},
        ),
        "dim_customer": (
            "t.customer_key = s.customer_key",
            {"city": "s.city", "department": "s.department", "created_at": "s.created_at"},
        ),
        "dim_location": ("t.location_key = s.location_key", None),
        "dim_device": ("t.device_key = s.device_key", None),
        "dim_datetime": ("t.datetime_key = s.datetime_key", None),
        "fact_funnel_event": ("t.event_date = s.event_date AND t.funnel_event_id = s.funnel_event_id", None),
        "fact_sales": (
            "t.event_date = s.event_date AND t.sale_line_id = s.sale_line_id",
            {"order_status": "s.order_status"},
        ),
        "agg_product_affinity": (
            "t.product_id = s.product_id AND t.related_product_id = s.related_product_id",
            {"co_orders": "s.co_orders", "rank": "s.rank"},
        ),
    }

    counts: dict[str, int] = {}
    for name, (rel, schema, _part) in schemas.items():
        pdf = star[name]
        counts[name] = len(pdf)
        if pdf.empty:
            continue
        sdf = spark.createDataFrame([_spark_record(r) for r in pdf.to_dict("records")], schema=schema)
        on_expr, upd = merge_keys[name]
        merge_delta_sql(
            spark,
            sdf,
            cfg.path(rel),
            on_expr,
            update_set=upd,
            insert=True,
            write_files=cfg.write_files,
        )
        log.info("Tabla Gold %s actualizada (%d filas procesadas)", name, len(pdf))

    return counts
