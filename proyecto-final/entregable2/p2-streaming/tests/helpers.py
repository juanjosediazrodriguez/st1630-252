"""Utilidades de pruebas: escribir escenarios de P1 como archivos por tramos y leer tablas Delta como pandas."""
from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd

from tiendacol_stream.config import Config


def make_cfg(root: Path, **kw) -> Config:
    base = dict(lake_root=str(root / "lake"), checkpoint_root=str(root / "ck"), source="files",
                input_path=str(root / "input"), run_once=True, sqs_mode="file",
                decisions_dir=str(root / "decisions"), shuffle_partitions=3,
                max_files_per_trigger=1, delta_max_files_per_trigger=1)
    base.update(kw)
    return Config(**base)


def write_slices(items, input_dir: Path, bounds: list[int] | None = None, n_slices: int = 4, prefix_start: int = 0,
                 single_stream: str | None = None) -> int:
    """Escribe los registros como archivos JSONL `<stream>_<tramo>.jsonl` (un tramo por micro-batch de Q1).

    `bounds` son cortes por posición (después de ordenar por `at`); sin ellos se parte en `n_slices` iguales.
    Los `event_ts` ya vienen fijados por P1, así que no hace falta esperar tiempo real.
    Con `single_stream` todos los registros de un tramo van a un solo archivo (menos micro-batches de Q1).
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    items = sorted(items, key=lambda s: s.at)
    if bounds is None:
        step = max(1, -(-len(items) // n_slices))
        bounds = list(range(step, len(items), step))
    cuts = [0, *bounds, len(items)]
    files = 0
    for k, (a, b) in enumerate(zip(cuts, cuts[1:])):
        chunk = items[a:b]
        by_stream: dict[str, list[bytes]] = {}
        for it in chunk:
            by_stream.setdefault(single_stream or it.record.stream_type, []).append(it.record.payload())
        for stream, payloads in by_stream.items():
            f = input_dir / f"{stream}_{prefix_start + k:04d}.jsonl"
            f.write_bytes(b"\n".join(payloads) + b"\n")
            # La fuente de archivos de Spark ordena por fecha de modificación y, en empate, el orden es el del sistema
            # de archivos (arbitrario). Se fija una fecha creciente (2 s por tramo) para que los tramos se procesen en
            # orden cronológico, como llegarían desde Kinesis dentro de un shard.
            t = time.time() - 3600 + (prefix_start + k) * 2
            os.utime(f, (t, t))
            files += 1
    return files


def read_delta(spark, path: str, order_by: str | None = None) -> pd.DataFrame:
    df = spark.read.format("delta").load(path)
    if order_by:
        df = df.orderBy(order_by)
    return df.toPandas()


def iso_df(spark, path: str, ts_cols: list[str]):
    """Lee una tabla Delta y deja los timestamps como texto ISO-8601 UTC (formato del contrato y de los verificadores)."""
    from tiendacol_stream.util import iso_col
    df = spark.read.format("delta").load(path)
    for c in ts_cols:
        df = df.withColumn(c, iso_col(c))
    return df.toPandas()


def read_jsonl(path) -> list[dict]:
    import json
    p = Path(path)
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()] if p.exists() else []
