"""Punto de entrada de Glue: argumentos de Glue ignorados, las cinco consultas juntas y tablas creadas antes de arrancar."""
import importlib.util
from pathlib import Path

import pytest

from helpers import read_delta, read_jsonl, write_slices
from tiendacol_gen import scenarios as sc
from tiendacol_stream.masters import MastersError
from tiendacol_stream.tables import TABLES, ensure_tables
from tiendacol_stream.config import Config


def cargar_script():
    ruta = Path(__file__).resolve().parents[1] / "glue" / "tiendacol_streaming.py"
    spec = importlib.util.spec_from_file_location("tiendacol_streaming_glue", ruta)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def args(lake, *extra):
    return ["--JOB_ID", "j_1", "--JOB_RUN_ID", "jr_1", "--JOB_NAME", "tiendacol-streaming",   # los agrega Glue
            "--lake_root", str(lake / "lake"), "--checkpoint_root", str(lake / "ck"), "--source", "files",
            "--input_path", str(lake / "input"), "--sqs_mode", "file", "--decisions_dir", str(lake / "decisions"),
            "--run_once", "true", "--shuffle_partitions", "3", "--max_files_per_trigger", "1",
            "--delta_max_files_per_trigger", "1", *extra]


def test_el_job_corre_las_cinco_consultas_con_argumentos_de_glue(spark, lake, masters_p1):
    """Todas las consultas (por defecto) en una sola ejecución, a través del script de Glue."""
    s = sc.rf09(masters_p1)
    write_slices(s.items, lake / "input", n_slices=2)
    cargar_script().main(args(lake))
    cfg = Config(lake_root=str(lake / "lake"), checkpoint_root=str(lake / "ck"))
    assert len(read_delta(spark, cfg.silver)) == 20
    ventas = read_delta(spark, cfg.agg_sales_minute)
    tot = ventas[ventas["category_id"].isna()]
    assert len(tot) == 1 and int(tot.iloc[0]["orders"]) == 20 and tot.iloc[0]["sales_cop"] == 1_600_000.0
    for name, (rel, _schema, _part) in TABLES.items():          # las seis tablas existen (también las vacías)
        assert (lake / "lake" / rel / "_delta_log").exists(), name
    assert read_jsonl(lake / "decisions" / "alerts.jsonl") == []   # sin ventas de stock no hay alertas


def test_ensure_tables_es_idempotente(spark, tmp_path):
    cfg = Config(lake_root=str(tmp_path / "lake"), checkpoint_root=str(tmp_path / "ck"))
    assert set(ensure_tables(spark, cfg)) == set(TABLES)
    assert ensure_tables(spark, cfg) == []
    for rel in ("bronze/events", "silver/events", "quarantine/events"):
        assert spark.read.format("delta").load(cfg.path(rel)).count() == 0


def test_sin_maestros_el_job_falla_antes_de_arrancar(spark, tmp_path):
    (tmp_path / "input").mkdir()
    with pytest.raises(MastersError, match="Sin maestros"):
        cargar_script().main(args(tmp_path))
