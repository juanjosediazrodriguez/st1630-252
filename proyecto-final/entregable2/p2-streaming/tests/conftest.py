"""Fixtures compartidas: una SparkSession por sesión de pytest, maestros de P1 y utilidades de lectura."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "p1-generador"))   # tiendacol_gen (solo lectura)
sys.path.insert(0, str(Path(__file__).resolve().parent))  # helpers

os.environ.setdefault("TZ", "UTC")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    from tiendacol_stream.config import DELTA_CONF
    b = (SparkSession.builder.master("local[2]").appName("tiendacol-tests")
         .config("spark.driver.memory", "3g")
         .config("spark.sql.shuffle.partitions", "3")           # pocas particiones: el estado y los shuffles son chicos
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.catalogImplementation", "in-memory")  # sin metastore (la imagen trae el catálogo de Glue)
         .config("spark.ui.enabled", "false")
         .config("spark.databricks.delta.snapshotPartitions", "2")
         .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "5"))
    for k, v in DELTA_CONF.items():
        b = b.config(k, v)
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def _reset_holders():
    yield
    from tiendacol_stream.masters import reset_holders
    reset_holders()


@pytest.fixture(scope="session")
def masters_p1():
    """Maestros de P1 (con productos y clientes de prueba)."""
    from tiendacol_gen.masters import build_masters
    return build_masters(seed=1, n_products=300, n_customers=500, n_sellers=20)


@pytest.fixture
def lake(tmp_path, masters_p1):
    """Carpeta de lago + checkpoints + maestros escritos con `write_masters` de P1."""
    from tiendacol_gen.masters import write_masters
    lake = tmp_path / "lake"
    write_masters(masters_p1, lake / "landing" / "masters", "2026-11-09")
    return tmp_path
