"""Reglas de cuarentena (DISENO §3.2): una prueba por regla, más las 4 malformaciones de P1."""
import json
import random

import pytest

from tiendacol_gen.events import EventFactory
from tiendacol_gen.masters import build_masters
from tiendacol_stream.parse import parse_events

OK_TS = "2026-11-09T15:00:00.000Z"
MISSING = object()


def ev(**kw):
    base = {"event_id": "e1", "event_type": "page_view", "event_ts": OK_TS, "schema_version": "1.0",
            "session_id": "s1", "user_id": None, "product_id": "P1"}
    base.update(kw)
    return {k: v for k, v in base.items() if v is not MISSING}


def order(**kw):
    base = {"event_id": "o1", "event_type": "order_created", "event_ts": OK_TS, "schema_version": "1.0",
            "order_id": "O1", "status": "paid", "total": 100.0,
            "lines": [{"product_id": "P1", "quantity": 1, "unit_price": 100.0, "discount": 0.0}],
            "payment": {"method": "card", "amount": 100.0, "status": "approved"}}
    base.update(kw)
    return {k: v for k, v in base.items() if v is not MISSING}


def stock(**kw):
    base = {"event_id": "k1", "event_type": "stock_change", "event_ts": OK_TS, "schema_version": "1.0",
            "product_id": "P1", "warehouse_id": "BOG-01", "delta": -1, "reason": "sale"}
    base.update(kw)
    return {k: v for k, v in base.items() if v is not MISSING}


CASES = [
    # -- válidos
    (ev(), None),
    (ev(event_type="add_to_cart", quantity=2, unit_price=50000.0), None),
    (order(), None),
    (stock(), None),
    (ev(event_ts="2026-11-09T15:00:00Z"), None),                       # sin fracción
    # -- 1. invalid_json
    ("{\"event_id\": \"e1\", \"event_ty", "invalid_json"),
    ("no es json", "invalid_json"),
    ("[1, 2, 3]", "invalid_json"),
    # -- 2. campos básicos ausentes (o vacíos)
    (ev(event_id=MISSING), "missing_field:event_id"),
    (ev(event_id=""), "missing_field:event_id"),
    (ev(event_type=MISSING), "missing_field:event_type"),
    (ev(event_ts=MISSING), "missing_field:event_ts"),
    (ev(event_ts=None), "missing_field:event_ts"),
    # -- 3. tipo desconocido (P1: teleport)
    (ev(event_type="teleport"), "unknown_event_type"),
    # -- 4. tipos erróneos
    (ev(event_ts="ayer en la tarde"), "wrong_type:event_ts"),          # P1
    (ev(event_ts="2026-11-09"), "wrong_type:event_ts"),                # fecha sin hora
    (ev(event_ts=1762700000), "wrong_type:event_ts"),                  # epoch numérico: no es ISO-8601
    (ev(event_type="add_to_cart", quantity="mucho", unit_price=1.0), "wrong_type:quantity"),
    (ev(event_type="add_to_cart", quantity=1, unit_price="gratis"), "wrong_type:unit_price"),
    (stock(delta="uno"), "wrong_type:delta"),
    (order(total="caro"), "wrong_type:total"),
    (order(lines="no-es-arreglo"), "wrong_type:lines"),
    (order(lines=[{"product_id": "P1", "quantity": "x", "unit_price": 1.0, "discount": 0.0}]), "wrong_type:lines"),
    # -- 5. campos requeridos por tipo
    (ev(session_id=MISSING), "missing_field:session_id"),              # P1: quita session_id
    (ev(event_type="add_to_cart", unit_price=1.0, quantity=1, product_id=MISSING), "missing_field:product_id"),
    (ev(event_type="add_to_cart", product_id="P1", unit_price=1.0), "missing_field:quantity"),
    (ev(event_type="add_to_cart", product_id="P1", quantity=1), "missing_field:unit_price"),
    (ev(event_type="remove_from_cart", product_id="P1"), "missing_field:quantity"),
    (ev(event_type="purchase"), "missing_field:order_id"),
    (order(order_id=MISSING), "missing_field:order_id"),
    (order(total=MISSING), "missing_field:total"),
    (order(lines=MISSING), "missing_field:lines"),
    (order(lines=[]), "missing_field:lines"),
    (order(event_type="order_status_changed", lines=MISSING, status=MISSING), "missing_field:status"),
    (stock(delta=MISSING), "missing_field:delta"),                     # P1: quita delta
    (stock(warehouse_id=MISSING), "missing_field:warehouse_id"),
    (stock(reason=MISSING), "missing_field:reason"),
    # -- 6. versión de esquema
    (ev(schema_version="2.0"), "unsupported_schema_version"),
    (ev(schema_version=MISSING), None),                                # si falta se asume "1.0"
    # -- orden de las reglas: gana la primera que falla
    (ev(event_type="teleport", event_ts="ayer"), "unknown_event_type"),
    (ev(event_ts="ayer", session_id=MISSING), "wrong_type:event_ts"),
    (ev(session_id=MISSING, schema_version="9"), "missing_field:session_id"),
]


def _raw(spark, payloads):
    rows = [(p if isinstance(p, str) else json.dumps(p), "clickstream", "k", f"seq-{i}", None) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, "raw string, stream string, partition_key string, sequence_number string, shard_id string")


def test_cada_regla_de_cuarentena(spark):
    out = parse_events(_raw(spark, [c[0] for c in CASES])).select("sequence_number", "reject_reason").collect()
    got = {r.sequence_number: r.reject_reason for r in out}
    errores = [(CASES[i][0], CASES[i][1], got[f"seq-{i}"]) for i in range(len(CASES)) if got[f"seq-{i}"] != CASES[i][1]]
    assert not errores, errores


def test_malformaciones_de_p1_caen_en_las_reglas_documentadas(spark):
    """Las cuatro causas de `EventFactory._malform` (missing_field, wrong_type, unknown_type, truncated_json)."""
    m = build_masters(seed=1, n_products=50, n_customers=50, n_sellers=5)
    f = EventFactory(m, 7, dup_rate=0, late_rate=0, malformed_rate=0)
    s = f._new_session()
    from datetime import datetime, timezone
    now = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)
    bases = [f.click(s, "page_view", now, product_id="P000001"),
             f.order_created("O1", s.user_id, s.city, s.department,
                             [{"product_id": "P000001", "quantity": 1, "unit_price": 10.0, "discount": 0.0}], now),
             f.stock_change("P000001", "BOG-01", -1, "sale", now)]
    esperado = {"missing_field": ("missing_field:",), "wrong_type": ("wrong_type:event_ts",),
                "unknown_type": ("unknown_event_type",), "truncated_json": ("invalid_json",)}
    recs, causas = [], []
    for _ in range(300):
        r = f._malform(random.choice(bases))
        recs.append(r.payload().decode("utf-8"))
        causas.append(r.flags[0].split(":")[1])
    assert set(causas) == set(esperado)                      # las cuatro causas aparecieron
    out = parse_events(_raw(spark, recs)).select("sequence_number", "reject_reason").collect()
    got = {r.sequence_number: r.reject_reason for r in out}
    for i, causa in enumerate(causas):
        razon = got[f"seq-{i}"]
        assert razon is not None, (recs[i], causa)           # ninguno pasa como válido
        assert razon.startswith(esperado[causa]), (causa, razon, recs[i])


def test_eventos_validos_de_p1_no_se_rechazan(spark):
    from datetime import datetime, timezone
    m = build_masters(seed=1, n_products=50, n_customers=50, n_sellers=5)
    f = EventFactory(m, 11, dup_rate=0, late_rate=0, malformed_rate=0)
    recs = f.next_batch(500, now=datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc))
    out = parse_events(_raw(spark, [r.payload().decode() for r in recs])).where("reject_reason is not null").collect()
    assert not out, [(r.raw[:80], r.reject_reason) for r in out[:3]]
