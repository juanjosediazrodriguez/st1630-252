"""Maestros vigentes (RF-06): products, customers, sellers e inventory del día más reciente.

Se busca el `_manifest_dt=*.json` más nuevo bajo `landing/masters/`. P1 sube el manifiesto al final,
así que si existe, la carga de ese día está completa. Los maestros son pequeños: se cargan una vez,
se dejan en caché y se usan en un *broadcast join* (la tabla chica viaja a cada ejecutor y el evento
no se mueve en un shuffle). Cada `masters_refresh_s` el job revisa si hay un manifiesto más nuevo.
"""
from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from .config import Config
from .util import glob_paths, read_text

_MANIFEST_RE = re.compile(r"_manifest_dt=(\d{4}-\d{2}-\d{2})\.json$")

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()), StructField("name", StringType()),
    StructField("category_id", IntegerType()), StructField("category", StringType()),
    StructField("seller_id", StringType()), StructField("price", DoubleType()),
    StructField("attributes", StringType()), StructField("updated_at", StringType())])
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()), StructField("city", StringType()),
    StructField("department", StringType()), StructField("created_at", StringType())])
SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType()), StructField("name", StringType()),
    StructField("city", StringType()), StructField("department", StringType())])
INVENTORY_SCHEMA = StructType([
    StructField("product_id", StringType()), StructField("warehouse_id", StringType()),
    StructField("stock", IntegerType())])


@dataclass
class Masters:
    dt: str
    manifest: str
    products: DataFrame     # product_id, product_name, category_id, category, seller_id, list_price
    customers: DataFrame    # customer_id, city, department
    sellers: DataFrame      # seller_id, seller_name, city, department
    inventory: DataFrame    # product_id, warehouse_id, base_stock
    categories: DataFrame   # category_id, category

    def unpersist(self):
        for df in (self.products, self.customers, self.sellers, self.inventory, self.categories):
            df.unpersist()


class MastersError(RuntimeError):
    pass


def latest_manifest(spark, masters_root: str) -> tuple[str, str] | None:
    """(día, ruta del manifiesto) más reciente, o None si no hay ninguno."""
    found = []
    for p in glob_paths(spark, f"{masters_root}/_manifest_dt=*.json"):
        m = _MANIFEST_RE.search(p)
        if m:
            found.append((m.group(1), p))
    return max(found) if found else None


def _read_entity(spark, root: str, rel: str, schema: StructType) -> DataFrame:
    path = f"{root}/{rel}"
    if rel.endswith(".json"):
        # P1 puede escribir products como JSON (arreglo) si el manifiesto lo indica; attributes llega como objeto.
        raw = spark.read.option("multiLine", True).json(path)
        cols = []
        for f in schema.fields:
            if f.name not in raw.columns:
                c = F.lit(None)
            elif f.name == "attributes" and isinstance(raw.schema["attributes"].dataType, StructType):
                c = F.to_json(F.col("attributes"))
            else:
                c = F.col(f.name)
            cols.append(c.cast(f.dataType).alias(f.name))
        return raw.select(*cols)
    # CSV con `attributes` en JSON: comillas dobles escapadas con comillas dobles; puede traer comas.
    return (spark.read.option("header", True).option("quote", '"').option("escape", '"')
            .option("multiLine", True).schema(schema).csv(path))


def load_masters(spark, masters_root: str) -> Masters:
    found = latest_manifest(spark, masters_root)
    if found is None:
        raise MastersError(
            f"No hay maestros: no se encontró ningún _manifest_dt=*.json bajo {masters_root}. "
            "Sin maestros no se cumple el RF-06; ejecute primero la carga diaria de maestros (P1).")
    dt, manifest_path = found
    manifest = json.loads(read_text(spark, manifest_path))
    files = manifest.get("files", {})
    for need in ("products", "customers", "inventory"):
        if need not in files:
            raise MastersError(f"El manifiesto {manifest_path} no lista '{need}'")
    r = lambda name, schema: _read_entity(spark, masters_root, files[name], schema)  # noqa: E731
    products = (r("products", PRODUCTS_SCHEMA)
                .select("product_id", F.col("name").alias("product_name"), "category_id", "category",
                        "seller_id", F.col("price").alias("list_price"))
                .dropDuplicates(["product_id"]))          # un maestro duplicado multiplicaría filas en el join
    customers = (r("customers", CUSTOMERS_SCHEMA).select("customer_id", "city", "department")
                 .dropDuplicates(["customer_id"]))
    sellers = (r("sellers", SELLERS_SCHEMA).select("seller_id", F.col("name").alias("seller_name"), "city", "department")
               if "sellers" in files else spark.createDataFrame([], "seller_id string, seller_name string, city string, department string"))
    inventory = (r("inventory", INVENTORY_SCHEMA)
                 .select("product_id", "warehouse_id", F.col("stock").alias("base_stock"))
                 .dropDuplicates(["product_id", "warehouse_id"]))
    categories = products.select("category_id", "category").where(F.col("category_id").isNotNull()).distinct()
    # cache + count: se materializan ahora; los micro-batches no vuelven a leer los CSV.
    for df in (products, customers, sellers, inventory, categories):
        df.cache().count()
    return Masters(dt, manifest_path, products, customers, sellers, inventory, categories)


class MastersHolder:
    """Mantiene los maestros vigentes en el driver y los recarga si aparece un manifiesto más nuevo."""

    def __init__(self, spark, cfg: Config):
        self.spark, self.cfg = spark, cfg
        self._lock = threading.Lock()
        self._m = load_masters(spark, cfg.masters_root)   # falla con error claro si no hay maestros
        self._checked = time.time()

    def get(self) -> Masters:
        with self._lock:
            return self._m

    def refresh_if_due(self, now: float | None = None) -> bool:
        """Solo la consulta de ingesta lo llama (una vez por micro-batch). Devuelve True si recargó."""
        now = time.time() if now is None else now
        with self._lock:
            if now - self._checked < self.cfg.masters_refresh_s:
                return False
            self._checked = now
            found = latest_manifest(self.spark, self.cfg.masters_root)
            if found is None or found[0] <= self._m.dt:
                return False
            old, self._m = self._m, load_masters(self.spark, self.cfg.masters_root)
            old.unpersist()
            return True


_HOLDERS: dict[str, MastersHolder] = {}
_HOLDERS_LOCK = threading.Lock()


def get_holder(spark, cfg: Config) -> MastersHolder:
    """Un solo holder por ruta de maestros: las cinco consultas comparten los mismos DataFrames en caché."""
    with _HOLDERS_LOCK:
        h = _HOLDERS.get(cfg.masters_root)
        if h is None:
            h = _HOLDERS[cfg.masters_root] = MastersHolder(spark, cfg)
        return h


def reset_holders() -> None:
    with _HOLDERS_LOCK:
        for h in _HOLDERS.values():
            try:
                h.get().unpersist()
            except Exception:  # noqa: BLE001 - limpieza best-effort (pruebas)
                pass
        _HOLDERS.clear()
