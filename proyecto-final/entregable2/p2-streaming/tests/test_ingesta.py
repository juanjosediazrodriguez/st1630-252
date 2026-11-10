"""Q1: enriquecimiento, deduplicación (en el lote y entre lotes) e idempotencia del reintento."""
import json
from datetime import datetime, timezone

import pytest

from helpers import make_cfg, read_delta
from tiendacol_stream.ingesta import process_batch
from tiendacol_stream.masters import MastersError, load_masters
from tiendacol_stream.tables import ensure_tables

TS = "2026-11-09T15:00:00.000Z"
RAW_SCHEMA = ("raw string, stream string, partition_key string, sequence_number string, shard_id string, "
              "arrival_ts timestamp")


class StaticHolder:
    """Sustituye al MastersHolder: maestros fijos, sin recargas."""

    def __init__(self, m):
        self.m = m

    def get(self):
        return self.m

    def refresh_if_due(self):
        return False


def frame(spark, payloads, arrival=datetime(2026, 11, 9, 15, 0, 5)):
    rows = [(p if isinstance(p, str) else json.dumps(p), "clickstream", "k", f"s{i}", None, arrival)
            for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, RAW_SCHEMA)


def click(eid, t="page_view", **kw):
    e = {"event_id": eid, "event_type": t, "event_ts": TS, "schema_version": "1.0", "session_id": "s1",
         "user_id": None, "device": "app", "city": "Bogotá", "department": "Cundinamarca", "product_id": None}
    e.update(kw)
    return e


@pytest.fixture
def env(spark, lake, masters_p1):
    cfg = make_cfg(lake)
    ensure_tables(spark, cfg)
    return cfg, StaticHolder(load_masters(spark, cfg.masters_root)), masters_p1


def silver(spark, cfg):
    return read_delta(spark, cfg.silver)


# ------------------------------------------------------------------ enriquecimiento (RF-06)
def test_ciudad_del_maestro_si_el_usuario_esta_identificado(spark, env):
    cfg, holder, m = env
    cust = m.customers["U-TEST-B"]                       # Medellín en el maestro
    batch = frame(spark, [
        click("a1", user_id="U-TEST-B", city="Atlantis", department="Nada"),   # identificado, ciudad del evento falsa
        click("a2", user_id="U-NO-EXISTE", city="Cali", department="Valle del Cauca"),  # identificado fuera del maestro
        click("a3", user_id=None, city="Cartagena", department="Bolívar"),               # anónimo
    ])
    print("TIEMPOS", process_batch(spark, batch, 0, cfg, holder))
    s = silver(spark, cfg).set_index("event_id")
    assert (s.loc["a1", "city"], s.loc["a1", "department"], s.loc["a1", "geo_source"]) == (
        cust["city"], cust["department"], "customer_master")
    assert (s.loc["a2", "city"], s.loc["a2", "geo_source"]) == ("Cali", "event")
    assert (s.loc["a3", "city"], s.loc["a3", "geo_source"], bool(s.loc["a3", "is_identified"])) == ("Cartagena", "event", False)
    assert bool(s.loc["a1", "is_identified"]) and bool(s.loc["a2", "is_identified"])
    assert (s["masters_dt"] == "2026-11-09").all()


def test_categoria_y_vendedor_por_producto_y_por_linea(spark, env):
    cfg, holder, m = env
    p1, p2 = m.products["TEST-RF09-A"], m.products["TEST-RF09-B"]       # categorías 1 y 3
    order = {"event_id": "o1", "event_type": "order_created", "event_ts": TS, "schema_version": "1.0",
             "order_id": "O1", "user_id": None, "city": "Cali", "department": "Valle del Cauca", "status": "paid",
             "lines": [{"product_id": p1["product_id"], "quantity": 2, "unit_price": 100000.0, "discount": 0.0},
                       {"product_id": p2["product_id"], "quantity": 1, "unit_price": 50000.0, "discount": 0.1},
                       {"product_id": "P-FANTASMA", "quantity": 1, "unit_price": 10.0, "discount": 0.0}],
             "total": 250000.0, "payment": {"method": "card", "amount": 250000.0, "status": "approved"}}
    stock = {"event_id": "k1", "event_type": "stock_change", "event_ts": TS, "schema_version": "1.0",
             "product_id": p1["product_id"], "warehouse_id": "BOG-01", "delta": -1, "reason": "sale"}
    process_batch(spark, frame(spark, [order, stock, click("c1", "page_view", product_id=p2["product_id"])]), 0, cfg, holder)
    rows = {r["event_id"]: r for r in silver(spark, cfg).to_dict("records")}
    k = rows["k1"]                                                       # por producto (stock_change)
    assert (k["category_id"], k["category"], k["seller_id"], k["product_name"], k["list_price"]) == (
        p1["category_id"], p1["category"], p1["seller_id"], p1["name"], float(p1["price"]))
    assert (k["delta"], k["reason"], k["warehouse_id"], k["stream"]) == (-1, "sale", "BOG-01", "stock_changes")
    c = rows["c1"]
    assert (c["category_id"], c["seller_id"]) == (p2["category_id"], p2["seller_id"])
    o = rows["o1"]                                                       # por línea, en el orden original
    lines = [dict(l.asDict()) if hasattr(l, "asDict") else dict(l) for l in o["lines"]]
    assert [l["product_id"] for l in lines] == [p1["product_id"], p2["product_id"], "P-FANTASMA"]
    assert (lines[0]["category_id"], lines[0]["seller_id"], lines[0]["line_value"]) == (p1["category_id"], p1["seller_id"], 200000.0)
    assert (lines[1]["category_id"], lines[1]["line_value"]) == (p2["category_id"], pytest.approx(45000.0))   # descuento 10 %
    assert lines[2]["category_id"] is None or lines[2]["category_id"] != lines[2]["category_id"]               # producto desconocido
    assert (o["total"], o["payment_method"], o["payment_amount"], o["payment_status"], o["stream"]) == (
        250000.0, "card", 250000.0, "approved", "orders")


def test_event_date_en_hora_de_colombia_y_tardios(spark, env):
    cfg, holder, _ = env
    # 2026-11-10T03:30Z = 2026-11-09 22:30 en Bogotá (UTC-5)
    tarde = click("t1", event_ts="2026-11-10T03:30:00.000Z")
    process_batch(spark, frame(spark, [tarde, click("t2")], arrival=datetime(2026, 11, 10, 3, 33, 0)), 0, cfg, holder)
    s = silver(spark, cfg).set_index("event_id")
    assert str(s.loc["t1", "event_date"]) == "2026-11-09"
    assert bool(s.loc["t1", "is_late"]) is True            # llegó 3 min después de su event_ts
    assert bool(s.loc["t2", "is_late"]) is True            # llegó 2026-11-10T03:33 vs event_ts 15:00 del día 9


# ------------------------------------------------------------------ cuarentena y Bronze
def test_cuarentena_y_bronze_en_un_lote(spark, env):
    cfg, holder, _ = env
    buenos = [click("v1"), click("v2")]
    malos = ["{\"event_id\": \"x\", \"ev", click("m1", event_type="teleport"), click("m2", event_ts="ayer en la tarde"),
             {k: v for k, v in click("m3").items() if k != "session_id"}]
    stats = process_batch(spark, frame(spark, buenos + malos), 0, cfg, holder)
    assert stats["received"] == 6
    b = read_delta(spark, cfg.bronze)
    q = read_delta(spark, cfg.quarantine)
    assert len(b) == 6 and len(q) == 4                                    # Bronze conserva todo
    assert set(q["reason"]) == {"invalid_json", "unknown_event_type", "wrong_type:event_ts", "missing_field:session_id"}
    assert q.loc[q["reason"] == "invalid_json", "event_id"].isna().all()   # el JSON roto no tiene id
    assert sorted(silver(spark, cfg)["event_id"]) == ["v1", "v2"]
    assert set(b["stream"]) == {"clickstream"} and b["batch_id"].eq(0).all()


# ------------------------------------------------------------------ deduplicación e idempotencia
def test_deduplicacion_en_el_lote_y_entre_lotes(spark, env):
    cfg, holder, _ = env
    a, b, c = click("A"), click("B"), click("C")
    process_batch(spark, frame(spark, [a, b, a]), 0, cfg, holder)          # A repetido dentro del lote
    assert sorted(silver(spark, cfg)["event_id"]) == ["A", "B"]
    process_batch(spark, frame(spark, [b, c, c]), 1, cfg, holder)          # B ya estaba (lote anterior); C repetido
    assert sorted(silver(spark, cfg)["event_id"]) == ["A", "B", "C"]
    assert len(read_delta(spark, cfg.bronze)) == 6                         # Bronze sí conserva los repetidos


def test_reintentar_el_mismo_lote_no_duplica_nada(spark, env):
    cfg, holder, _ = env
    batch = frame(spark, [click("A"), click("B"), click("A"), "{roto"])
    process_batch(spark, batch, 0, cfg, holder, qid="q1")
    antes = (len(read_delta(spark, cfg.bronze)), len(read_delta(spark, cfg.quarantine)), len(silver(spark, cfg)))
    process_batch(spark, batch, 0, cfg, holder, qid="q1")                  # Glue reintenta el micro-batch 0
    process_batch(spark, batch, 0, cfg, holder, qid="q1")
    despues = (len(read_delta(spark, cfg.bronze)), len(read_delta(spark, cfg.quarantine)), len(silver(spark, cfg)))
    assert antes == (4, 1, 2) and despues == antes


def test_lote_vacio_no_escribe(spark, env):
    cfg, holder, _ = env
    stats = process_batch(spark, frame(spark, []), 0, cfg, holder)
    assert stats["received"] == 0 and len(silver(spark, cfg)) == 0


# ------------------------------------------------------------------ maestros
def test_sin_maestros_falla_con_error_claro(spark, tmp_path):
    cfg = make_cfg(tmp_path)
    with pytest.raises(MastersError, match="RF-06"):
        load_masters(spark, cfg.masters_root)
