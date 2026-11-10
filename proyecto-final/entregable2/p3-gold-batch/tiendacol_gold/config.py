"""Configuración del job de Glue Batch de P3 (idénticos argumentos en AWS Glue y en local).

AWS Glue inyecta argumentos propios (`--JOB_ID`, `--JOB_RUN_ID`, `--JOB_NAME`, ...); se usa
`parse_known_args` para ignorarlos sin fallar al iniciar.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, fields


def _opt(v):
    """Glue pasa cadenas vacías para parámetros opcionales vacíos."""
    return None if v is None or str(v).strip() == "" else str(v).strip()


@dataclass
class GoldConfig:
    lake_root: str
    masters_root: str = ""
    process_date: str | None = None         # Si se indica (AAAA-MM-DD), filtra Silver por event_date
    shuffle_partitions: int = 8
    write_files: int = 1                    # coalesce antes de escribir para evitar small files en S3
    top_affinity_k: int = 10                # Top-K productos relacionados por producto en RF-13

    def __post_init__(self):
        self.lake_root = self.lake_root.rstrip("/")
        if not self.masters_root:
            self.masters_root = f"{self.lake_root}/landing/masters"
        self.masters_root = self.masters_root.rstrip("/")

    def path(self, rel: str) -> str:
        return f"{self.lake_root}/{rel}"

    # Entrada escrita por P2
    @property
    def silver_events(self) -> str:
        return self.path("silver/events")

    @property
    def bronze_events(self) -> str:
        return self.path("bronze/events")

    # Dimensiones de Gold (escritas por P3)
    @property
    def dim_product(self) -> str:
        return self.path("gold/dim_product")

    @property
    def dim_seller(self) -> str:
        return self.path("gold/dim_seller")

    @property
    def dim_customer(self) -> str:
        return self.path("gold/dim_customer")

    @property
    def dim_location(self) -> str:
        return self.path("gold/dim_location")

    @property
    def dim_device(self) -> str:
        return self.path("gold/dim_device")

    @property
    def dim_datetime(self) -> str:
        return self.path("gold/dim_datetime")

    # Hechos y agregados de Gold (escritos por P3)
    @property
    def fact_funnel_event(self) -> str:
        return self.path("gold/fact_funnel_event")

    @property
    def fact_sales(self) -> str:
        return self.path("gold/fact_sales")

    @property
    def agg_product_affinity(self) -> str:
        return self.path("gold/agg_product_affinity")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TiendaCol P3 Gold Batch (SCD2 + Esquema Estrella)")
    a = p.add_argument
    a("--lake_root", required=True, help="Raíz del lago (s3://<bucket> o ruta local)")
    a("--masters_root", default="", help="Raíz de maestros (por defecto <lake_root>/landing/masters)")
    a("--process_date", default="", help="Fecha opcional AAAA-MM-DD para reprocesar un día de Silver")
    a("--shuffle_partitions", type=int, default=8)
    a("--write_files", type=int, default=1)
    a("--top_affinity_k", type=int, default=10)
    return p


def parse_config(argv: list[str] | None = None) -> GoldConfig:
    ns, _unknown = build_parser().parse_known_args(argv)
    d = vars(ns)
    d["process_date"] = _opt(d["process_date"])
    valid = {f.name for f in fields(GoldConfig)}
    return GoldConfig(**{k: v for k, v in d.items() if k in valid})


DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def build_spark(app_name: str = "tiendacol-gold-batch", master: str | None = None, extra: dict | None = None):
    """Inicializa o recupera la SparkSession con extensiones de Delta Lake."""
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    for k, v in {**DELTA_CONF, **(extra or {})}.items():
        b = b.config(k, v)
    return b.getOrCreate()


def apply_runtime_conf(spark, cfg: GoldConfig) -> None:
    """Ajusta zona horaria UTC y particiones de shuffle para baja latencia en Glue."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
    spark.conf.set("spark.databricks.delta.snapshotPartitions", str(cfg.shuffle_partitions))
