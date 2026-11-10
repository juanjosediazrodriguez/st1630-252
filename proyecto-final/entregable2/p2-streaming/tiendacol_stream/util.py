"""Utilidades pequeñas compartidas por las consultas."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import Column, functions as F

BOGOTA = "America/Bogota"
ISO_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


def iso_col(c) -> Column:
    """Timestamp -> texto ISO-8601 UTC con milisegundos (formato del contrato). Se formatea en Spark porque
    `collect()` convierte los timestamps a la zona local del proceso Python y se perdería la garantía UTC."""
    return F.date_format(F.col(c) if isinstance(c, str) else c, ISO_FMT)


def now_lit() -> Column:
    """Hora del micro-batch como constante (no `current_timestamp()`): vale lo mismo en todo el plan."""
    return F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp")


def view_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def delta_ident(path: str) -> str:
    """Ruta como identificador de SQL: delta.`ruta`."""
    return "delta.`" + path.replace("`", "") + "`"


def _fs(spark, path: str):
    jvm = spark._jvm
    uri = jvm.java.net.URI(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration()), jvm.org.apache.hadoop.fs.Path(path)


def path_exists(spark, path: str) -> bool:
    """Existe la ruta (local, s3://, ...) según el sistema de archivos de Hadoop."""
    fs, p = _fs(spark, path)
    return bool(fs.exists(p))


def glob_paths(spark, pattern: str) -> list[str]:
    """Rutas que cumplen el patrón (con comodines), ordenadas por nombre."""
    fs, p = _fs(spark, pattern)
    st = fs.globStatus(p)
    return sorted(s.getPath().toString() for s in st) if st is not None else []


def is_delta(spark, path: str) -> bool:
    return path_exists(spark, path.rstrip("/") + "/_delta_log")


def read_text(spark, path: str) -> str:
    return "\n".join(r.value for r in spark.read.text(path).collect())
