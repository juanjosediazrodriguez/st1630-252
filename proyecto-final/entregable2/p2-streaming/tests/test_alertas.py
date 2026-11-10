"""Q3 · alertas de agotamiento (RF-07): lógica pura, idempotencia del lote y escenario `rf07` de P1 de punta a punta."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from helpers import make_cfg, read_delta, read_jsonl, write_slices
from tiendacol_gen import scenarios as sc
from tiendacol_stream import alertas as A
from tiendacol_stream.job import run_job
from tiendacol_stream.sqs import SqsPublisher
from tiendacol_stream.tables import SILVER_SCHEMA, STOCK_SCHEMA, ensure_tables

T0 = datetime(2026, 11, 9, 15, 0, 0)
WINDOW_T = "product_id string, product_name string, category_id int, window struct<start:timestamp,end:timestamp>, " \
           "units_15m long, last_sale_ts timestamp"


def at(minutes=0.0, seconds=0.0):
    return T0 + timedelta(minutes=minutes, seconds=seconds)


def parse_iso(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _put(spark, path, schema, rows):
    """Escribe filas (dicts) en una tabla Delta con el esquema completo; lo que falta queda nulo."""
    data = [tuple(r.get(f.name) for f in schema) for r in rows]
    spark.createDataFrame(data, schema).write.format("delta").mode("append").save(path)


def _lake_with(spark, tmp_path, base_stock, sales):
    """Lago mínimo: `stock_current` con el inventario base y Silver con `sales` = [(minuto, unidades)] de un producto."""
    cfg = make_cfg(tmp_path, sqs_mode="file")
    ensure_tables(spark, cfg)
    _put(spark, cfg.stock_current, STOCK_SCHEMA,
         [{"product_id": "P1", "warehouse_id": "W1", "base_stock": base_stock, "cumulative_delta": 0, "stock": base_stock}])
    _put(spark, cfg.silver, SILVER_SCHEMA,
         [{"event_id": f"e{i}", "event_type": "stock_change", "event_ts": at(m), "product_id": "P1", "warehouse_id": "W1",
           "delta": -u, "reason": "sale", "event_date": at(m).date()} for i, (m, u) in enumerate(sales)])
    return cfg


def _batch(spark, rows):
    return spark.createDataFrame(rows, WINDOW_T)


class FakePublisher(SqsPublisher):
    """Publicador en memoria; `fail` simula una caída de SQS."""

    def __init__(self, cfg, fail=False):
        super().__init__(cfg)
        self.fail, self.messages = fail, []

    def publish(self, kind, messages):
        if self.fail:
            raise RuntimeError("SQS caído")
        self.messages += messages
        return len(messages)


# ------------------------------------------------------------------ lógica pura
def test_ventana_vigente_es_una_por_producto_y_termina_en_el_minuto_siguiente_a_la_ultima_venta(spark):
    """Update mode emite todas las ventanas deslizantes tocadas (15 por venta): solo una es la vigente."""
    cfg = make_cfg(Path("/x"))
    ult = at(3, 20)                                   # última venta de P1 en el lote: 15:03:20
    rows = [("P1", "p", 1, (at(k - 15), at(k)), 40 + k, ult) for k in range(1, 20) if k != 4]
    rows += [("P1", "p", 1, (at(4 - 15), at(4)), 50, ult)]
    rows += [("P2", "q", 2, (at(-10), at(5)), 7, at(4, 10))]
    got = A.current_windows(_batch(spark, rows), cfg).collect()
    by = {r.product_id: r for r in got}
    assert len(got) == 2
    assert by["P1"].window_end == at(4) and by["P1"].units_15m == 50     # trunc(15:03:20) + 1 min
    assert by["P2"].window_end == at(5)


def test_unidades_de_ventana_deslizante_en_lote(spark):
    """15 min deslizando cada 1: una venta cae en 15 ventanas y las ventas suman |delta|."""
    df = spark.createDataFrame(
        [("stock_change", "sale", "P1", "n", 1, at(0), -2), ("stock_change", "sale", "P1", "n", 1, at(1), -3),
         ("stock_change", "restock", "P1", "n", 1, at(1), 50), ("page_view", None, "P1", "n", 1, at(1), None)],
        "event_type string, reason string, product_id string, product_name string, category_id int, event_ts timestamp, delta int")
    # En lote (sin streaming) `withWatermark` no hace nada: sirve para comprobar el agrupamiento.
    w = A.window_units(df, make_cfg(Path("/x")))
    cur = w.where(F.col("window.end") == F.lit(at(2))).collect()
    assert [(r.units_15m, r.last_sale_ts) for r in cur] == [(5, at(1))]   # solo ventas; reposición y vistas no cuentan
    assert w.where(F.col("window.end") == F.lit(at(1))).collect()[0].units_15m == 2
    assert w.count() == 16                                                # 15 ventanas de la 1ª venta + 1 más de la 2ª


def test_el_stock_sale_del_inventario_base_mas_las_ventas_hasta_la_ultima_venta(spark, tmp_path):
    """El stock no depende de cuánto se haya adelantado Q2 (corre en paralelo): base + deltas hasta `last_sale_ts`."""
    cfg = _lake_with(spark, tmp_path, 100, [(0, 3), (1, 4), (9, 50)])   # la venta del min 9 es posterior a la ventana
    c = _batch(spark, [("P1", "p", 1, (at(-14), at(2)), 7, at(1, 30))]).select(
        "product_id", "product_name", "category_id", F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"), "units_15m", "last_sale_ts")
    (r,) = A.with_stock(spark, c, cfg).collect()
    assert r.stock == 100 - 3 - 4


# ------------------------------------------------------------------ lote: idempotencia y orden de publicación
def _win(units, last_sale):
    end = last_sale.replace(second=0) + timedelta(minutes=1)
    return ("P1", "Producto uno", 1, (end - timedelta(minutes=15), end), units, last_sale)


def test_reintentar_el_mismo_lote_no_duplica_la_alerta(spark, tmp_path):
    cfg = _lake_with(spark, tmp_path, 100, [(0, 60)])                   # stock 40, 60 unidades -> cobertura 10 min
    pub = FakePublisher(cfg)
    lote = _batch(spark, [_win(60, at(0, 30))])
    assert A.process_batch(spark, lote, 7, cfg, pub) == 1
    assert A.process_batch(spark, lote, 7, cfg, pub) == 0               # el reintento: ya está en alert_log (enfriamiento)
    assert len(pub.messages) == 1 and pub.messages[0]["alert_id"] == "P1|2026-11-09T15:01:00.000Z"
    assert read_delta(spark, cfg.alert_log)["alert_id"].tolist() == ["P1|2026-11-09T15:01:00.000Z"]


def test_si_sqs_falla_no_se_registra_y_el_reintento_publica_el_mismo_alert_id(spark, tmp_path):
    """Primero SQS y luego alert_log: una caída entre ambos pasos pierde nada (at-least-once, id determinista)."""
    cfg = _lake_with(spark, tmp_path, 100, [(0, 60)])
    lote = _batch(spark, [_win(60, at(0, 30))])
    with pytest.raises(RuntimeError):
        A.process_batch(spark, lote, 7, cfg, FakePublisher(cfg, fail=True))
    assert read_delta(spark, cfg.alert_log).empty                         # nada quedó registrado
    ok = FakePublisher(cfg)
    assert A.process_batch(spark, lote, 7, cfg, ok) == 1
    assert ok.messages[0]["alert_id"] == "P1|2026-11-09T15:01:00.000Z"


def test_enfriamiento_de_60_minutos_y_una_alerta_por_producto_y_lote(spark, tmp_path):
    cfg = _lake_with(spark, tmp_path, 100, [(0, 60), (30, 1), (70, 1)])
    pub = FakePublisher(cfg)
    assert A.process_batch(spark, _batch(spark, [_win(60, at(0, 30))]), 1, cfg, pub) == 1
    # 30 min después, aún alerta por cobertura pero sigue el enfriamiento
    assert A.process_batch(spark, _batch(spark, [_win(60, at(30, 5))]), 2, cfg, pub) == 0
    # 70 min después ya pasaron los 60 min: se vuelve a alertar
    assert A.process_batch(spark, _batch(spark, [_win(60, at(70, 5))]), 3, cfg, pub) == 1
    assert [m["last_sale_ts"][11:16] for m in pub.messages] == ["15:00", "16:10"]


def test_mensaje_cumple_el_contrato(spark, tmp_path):
    cfg = _lake_with(spark, tmp_path, 100, [(0, 60)])
    pub = FakePublisher(cfg)
    A.process_batch(spark, _batch(spark, [_win(60, at(0, 30))]), 1, cfg, pub)
    (m,) = pub.messages
    assert set(m) == {"schema_version", "decision_type", "alert_id", "product_id", "product_name", "category_id", "stock",
                      "units_15m", "velocity_per_min", "coverage_min", "window_start", "window_end", "last_sale_ts",
                      "detected_at"}
    assert (m["stock"], m["units_15m"], m["velocity_per_min"], m["coverage_min"]) == (40, 60, 4.0, 10.0)
    assert m["window_end"] == "2026-11-09T15:01:00.000Z" and m["window_start"] == "2026-11-09T14:46:00.000Z"


# ------------------------------------------------------------------ escenario rf07 de P1, de punta a punta
@pytest.fixture(scope="module")
def rf07_run(spark, tmp_path_factory, masters_p1):
    """Silver construido por tramos (un archivo por micro-batch de Q1), después Q3 sobre Silver con un archivo por lote."""
    from tiendacol_gen.masters import write_masters
    root = tmp_path_factory.mktemp("rf07")
    write_masters(masters_p1, root / "lake" / "landing" / "masters", "2026-11-09")
    s = sc.rf07(masters_p1)
    cfg = make_cfg(root, queries=("ingesta",))
    # primer tramo: 60 ventas (2 eventos por venta); después un tramo por venta, de modo que cada lote de Q3 trae una venta
    bounds = [120] + [120 + 2 * k for k in range(1, 16)]
    write_slices(s.items, root / "input", bounds=bounds, single_stream="orders")
    run_job(cfg, spark)
    cfg_q3 = make_cfg(root, queries=("alertas",))
    run_job(cfg_q3, spark)
    return s, cfg_q3, root


def test_rf07_la_primera_alerta_sale_con_la_venta_65_y_no_con_la_64(rf07_run, spark):
    s, cfg, root = rf07_run
    e = s.expected
    assert e["trigger_index"] == 64
    alerts = read_jsonl(root / "decisions" / "alerts.jsonl")
    assert len(alerts) == 1, [a["last_sale_ts"] for a in alerts]       # el enfriamiento calla las ventas 66..75
    a = alerts[0]
    assert a["product_id"] == "TEST-RF07" and a["decision_type"] == "stock_depletion_alert"
    assert a["last_sale_ts"] == e["trigger_ts"]                         # venta #65, no la #64 (cobertura exacta de 60 min)
    assert (a["stock"], a["units_15m"]) == (255, 65) == (e["stock_at_trigger"], 65)
    assert abs(a["coverage_min"] - 58.8) < 0.05 and abs(a["velocity_per_min"] - 65 / 15) < 1e-3
    assert a["alert_id"] == f"TEST-RF07|{a['window_end']}"
    assert parse_iso(a["window_end"]) == parse_iso(a["last_sale_ts"]).replace(second=0, microsecond=0) + timedelta(minutes=1)
    log = read_delta(spark, cfg.alert_log)
    assert log["alert_id"].tolist() == [a["alert_id"]]


def test_rf07_reprocesar_con_otro_checkpoint_no_repite_la_alerta(rf07_run, spark):
    """Idempotencia de punta a punta: relee todo Silver con un checkpoint nuevo; alert_log recuerda la alerta."""
    s, cfg, root = rf07_run
    antes = read_jsonl(root / "decisions" / "alerts.jsonl")
    cfg2 = make_cfg(root, queries=("alertas",), checkpoint_root=str(root / "ck2"))
    run_job(cfg2, spark)
    assert read_jsonl(root / "decisions" / "alerts.jsonl") == antes
    assert len(read_delta(spark, cfg.alert_log)) == 1
