"""Configuración del job: los mismos argumentos en Glue y en local.

Glue agrega sus propios argumentos (`--JOB_ID`, `--JOB_RUN_ID`, `--JOB_NAME`, ...), por eso se usa
`parse_known_args`: lo desconocido se ignora en vez de romper el arranque.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, fields

ALL_QUERIES = ("ingesta", "stock", "alertas", "carritos", "ventas_minuto")


def _bool(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "si", "sí")


def _opt(v):
    """Glue entrega cadenas vacías para parámetros sin valor: se tratan como ausentes."""
    return None if v is None or str(v).strip() == "" else v


@dataclass
class Config:
    lake_root: str
    checkpoint_root: str
    masters_root: str = ""
    source: str = "kinesis"                 # kinesis | files | bronze
    stream_name: str = "tiendacol-events"
    region: str = "us-east-1"
    kinesis_endpoint: str | None = None
    starting_position: str = "TRIM_HORIZON"
    kinesis_format: str = "kinesis"
    input_path: str | None = None
    sqs_mode: str = "aws"                   # aws | file | off
    alerts_queue_url: str | None = None
    carts_queue_url: str | None = None
    decisions_dir: str | None = None
    ingest_trigger_s: int = 30
    downstream_trigger_s: int = 10
    run_once: bool = False                  # availableNow: procesa lo pendiente y termina (pruebas)
    queries: tuple = ALL_QUERIES
    shuffle_partitions: int = 8
    delta_max_files_per_trigger: int | None = None
    max_files_per_trigger: int | None = None  # solo fuente `files`
    # umbrales de DISENO.md (valores compartidos con el E1)
    alert_horizon_min: float = 60.0
    alert_window_min: int = 15
    alert_slide_min: int = 1
    alert_cooldown_min: float = 60.0
    alert_watermark: str = "1 minute"
    cart_min_value: float = 500_000.0
    cart_gap_min: int = 30
    cart_watermark: str = "3 minutes"
    sales_watermark: str = "1 minute"
    late_threshold_s: int = 60
    masters_refresh_s: int = 300
    write_files: int = 1                    # coalesce antes de escribir: menos archivos pequeños

    def __post_init__(self):
        self.lake_root = self.lake_root.rstrip("/")
        self.checkpoint_root = self.checkpoint_root.rstrip("/")
        if not self.masters_root:
            self.masters_root = f"{self.lake_root}/landing/masters"
        self.masters_root = self.masters_root.rstrip("/")

    # -- rutas ---------------------------------------------------------------
    def path(self, rel: str) -> str:
        return f"{self.lake_root}/{rel}"

    def checkpoint(self, name: str) -> str:
        return f"{self.checkpoint_root}/{name}"

    @property
    def bronze(self): return self.path("bronze/events")

    @property
    def quarantine(self): return self.path("quarantine/events")

    @property
    def silver(self): return self.path("silver/events")

    @property
    def stock_current(self): return self.path("silver/stock_current")

    @property
    def alert_log(self): return self.path("silver/alert_log")

    @property
    def agg_sales_minute(self): return self.path("gold/agg_sales_minute")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TiendaCol P2 streaming")
    a = p.add_argument
    a("--lake_root", required=True)
    a("--checkpoint_root", required=True)
    a("--masters_root", default="")
    a("--source", default="kinesis", choices=["kinesis", "files", "bronze"])
    a("--stream_name", default="tiendacol-events")
    a("--region", default="us-east-1")
    a("--kinesis_endpoint", default="")
    a("--starting_position", default="TRIM_HORIZON")
    a("--kinesis_format", default="kinesis")
    a("--input_path", default="")
    a("--sqs_mode", default="aws", choices=["aws", "file", "off"])
    a("--alerts_queue_url", default="")
    a("--carts_queue_url", default="")
    a("--decisions_dir", default="")
    a("--ingest_trigger_s", type=int, default=30)
    a("--downstream_trigger_s", type=int, default=10)
    a("--run_once", nargs="?", const="true", default="false")  # Glue exige `--clave valor`; ambas formas sirven
    a("--queries", default=",".join(ALL_QUERIES))
    a("--shuffle_partitions", type=int, default=8)
    a("--delta_max_files_per_trigger", default="")
    a("--max_files_per_trigger", default="")
    a("--alert_horizon_min", type=float, default=60.0)
    a("--alert_window_min", type=int, default=15)
    a("--alert_cooldown_min", type=float, default=60.0)
    a("--alert_watermark", default="1 minute")
    a("--cart_min_value", type=float, default=500_000.0)
    a("--cart_gap_min", type=int, default=30)
    a("--cart_watermark", default="3 minutes")
    a("--sales_watermark", default="1 minute")
    a("--late_threshold_s", type=int, default=60)
    a("--masters_refresh_s", type=int, default=300)
    a("--write_files", type=int, default=1)
    return p


def parse_config(argv: list[str] | None = None) -> Config:
    ns, _unknown = build_parser().parse_known_args(argv)
    d = vars(ns)
    d["run_once"] = _bool(d["run_once"])
    for k in ("kinesis_endpoint", "input_path", "alerts_queue_url", "carts_queue_url", "decisions_dir"):
        d[k] = _opt(d[k])
    for k in ("delta_max_files_per_trigger", "max_files_per_trigger"):
        d[k] = int(d[k]) if _opt(d[k]) is not None else None
    qs = tuple(q.strip() for q in d["queries"].split(",") if q.strip())
    bad = set(qs) - set(ALL_QUERIES)
    if bad:
        raise SystemExit(f"--queries desconocidas: {sorted(bad)}; válidas: {ALL_QUERIES}")
    d["queries"] = qs
    valid = {f.name for f in fields(Config)}
    return Config(**{k: v for k, v in d.items() if k in valid})


# -- Spark -------------------------------------------------------------------
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def build_spark(app_name: str = "tiendacol-streaming", master: str | None = None, extra: dict | None = None):
    """SparkSession con Delta. En Glue la sesión ya existe y las confs estáticas (extensiones de SQL)
    llegan por `--conf` del job (ver infra/glue_job.md); `getOrCreate` devuelve esa misma sesión."""
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName(app_name)
    if master:
        b = b.master(master)
    for k, v in {**DELTA_CONF, **(extra or {})}.items():
        b = b.config(k, v)
    return b.getOrCreate()


def apply_runtime_conf(spark, cfg: Config) -> None:
    """Confs de ejecución (sí se pueden cambiar con la sesión creada).

    - Zona UTC: todas las marcas de tiempo del contrato son UTC; `event_date` se calcula aparte (Colombia).
    - Pocas particiones de shuffle: con 200 (valor por defecto) cada operador con estado haría 200 tareas
      por micro-batch y la latencia se dispararía; el estado también se parte en esa cantidad.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
    # Delta reconstruye el estado de la tabla (log de transacciones) con 50 tareas por defecto: para tablas
    # pequeñas con commits cada pocos segundos eso es latencia pura. Se iguala a las particiones de shuffle.
    spark.conf.set("spark.databricks.delta.snapshotPartitions", str(cfg.shuffle_partitions))
