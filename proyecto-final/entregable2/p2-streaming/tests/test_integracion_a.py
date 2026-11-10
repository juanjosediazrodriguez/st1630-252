"""Fase A · integración: escenarios de P1 -> archivos por tramos -> job (Q1) -> Bronze/Silver/cuarentena -> verificadores de P1."""
import pytest

from helpers import make_cfg, read_delta, write_slices
from tiendacol_gen import scenarios as sc
from tiendacol_gen import verify as v
from tiendacol_stream.job import run_job


def correr_q1(spark, root, scenario, **kw):
    cfg = make_cfg(root, queries=("ingesta",))
    n = write_slices(scenario.items, root / "input", **kw)
    run_job(cfg, spark)
    return cfg, n


def tablas(spark, cfg):
    return read_delta(spark, cfg.bronze), read_delta(spark, cfg.silver), read_delta(spark, cfg.quarantine)


def test_rf05_validacion_cuarentena_y_deduplicacion(spark, lake, masters_p1):
    s = sc.rf05(masters_p1)                                  # 10.000 enviados: 9.800 únicos + 100 duplicados + 100 malformados
    cfg, _ = correr_q1(spark, lake, s, n_slices=4)
    bronze, silver, quarantine = tablas(spark, cfg)
    res = v.verify_rf05(s.expected, silver, quarantine)
    print(res)
    assert res.ok, res
    assert len(bronze) == 10_000                             # Bronze conserva todo lo recibido
    causas = quarantine["reason"].str.split(":").str[0].value_counts().to_dict()
    # las cuatro malformaciones de P1 terminan en cuarentena con causa legible
    assert sum(causas.values()) == 100 and set(causas) <= {"invalid_json", "missing_field", "wrong_type", "unknown_event_type"}
    assert silver["event_id"].is_unique


def test_rnf03_cero_perdidos_cero_duplicados(spark, lake, masters_p1):
    s = sc.rnf03(masters_p1)                                 # 10.000 + 100 reenviados
    cfg, _ = correr_q1(spark, lake, s, n_slices=4)
    bronze, silver, quarantine = tablas(spark, cfg)
    res = v.verify_rnf03(s.expected, bronze, silver)
    print(res)
    assert res.ok, res
    assert len(bronze) == 10_100 and len(silver) == 10_000 and len(quarantine) == 0


def test_rf01_bronze_conserva_los_eventos_identicos_y_rf06_enriquecimiento(spark, lake, masters_p1):
    s = sc.rf01(masters_p1, n=3000)
    cfg, _ = correr_q1(spark, lake, s, n_slices=3)
    bronze, silver, _q = tablas(spark, cfg)
    res1 = v.verify_rf01(s.expected, bronze)
    print(res1)
    assert res1.ok, res1
    res6 = v.verify_rf06(masters_p1, silver, n=1000)
    print(res6)
    assert res6.ok, res6
    assert len(silver) == 3000


def test_reproceso_desde_bronze_reconstruye_silver(spark, lake, masters_p1):
    """DISENO §6: con --source bronze el mismo código de validación reconstruye Silver sin tocar Bronze."""
    s = sc.rnf03(masters_p1, n=1500, resent=30)
    cfg, _ = correr_q1(spark, lake, s, n_slices=3)
    antes_b = len(read_delta(spark, cfg.bronze))
    silver_ids = set(read_delta(spark, cfg.silver)["event_id"])
    # Silver nuevo y vacío: se borra la tabla y su checkpoint de reproceso es otro
    import shutil
    shutil.rmtree(cfg.silver.replace("file:", ""), ignore_errors=True)
    from tiendacol_stream.tables import ensure_tables
    ensure_tables(spark, cfg)
    cfg2 = make_cfg(lake, queries=("ingesta",), source="bronze")
    cfg2.checkpoint_root = str(lake / "ck_reproceso")
    run_job(cfg2, spark)
    assert len(read_delta(spark, cfg.bronze)) == antes_b                    # Bronze intacto
    silver2 = read_delta(spark, cfg.silver)
    assert set(silver2["event_id"]) == silver_ids and silver2["event_id"].is_unique
