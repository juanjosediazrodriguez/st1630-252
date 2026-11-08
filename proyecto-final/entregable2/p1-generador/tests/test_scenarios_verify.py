"""Los escenarios deben pasar sus propios verificadores con datos 'perfectos' y fallar con datos dañados."""
import json
from datetime import timedelta

import pandas as pd
import pytest

from tiendacol_gen import scenarios as sc
from tiendacol_gen import verify as v
from tiendacol_gen.sinks import FileSink, NullSink


@pytest.fixture(scope="module")
def m():
    from tiendacol_gen.masters import build_masters
    return build_masters(seed=1, n_products=300, n_customers=500, n_sellers=20)


def _events(s):
    return [i.record.event for i in s.items if i.record.event]


def _df(s):
    return pd.DataFrame([{"event_id": e["event_id"], "payload": json.dumps(e)} for e in _events(s)])


# ------------------------------------------------------------------ RF-01/02/03/05/RNF-03
def test_rf01_pass_and_fail(m):
    s = sc.rf01(m, n=500)
    assert len(s.items) == 500 and len({e["event_id"] for e in _events(s)}) == 500
    assert v.verify_rf01(s.expected, _df(s)).ok
    assert not v.verify_rf01(s.expected, _df(s).iloc[:-1]).ok  # falta uno
    tampered = _df(s)
    e = json.loads(tampered.loc[0, "payload"])
    e["city"] = "Atlantis"
    tampered.loc[0, "payload"] = json.dumps(e)
    assert not v.verify_rf01(s.expected, tampered).ok  # contenido distinto


def test_rf02_counts_and_verify(m):
    s = sc.rf02(m)
    kinds = [i.record.event["event_type"] for i in s.items]
    assert kinds.count("order_created") == 100 and kinds.count("order_status_changed") == 20
    assert v.verify_rf02(s.expected, _df(s)).ok
    assert not v.verify_rf02(s.expected, _df(s).iloc[5:]).ok


def test_rf03_expected_stock_matches_manual_sum(m):
    s = sc.rf03(m)
    assert len(s.items) == 50
    totals = {}
    for e in _events(s):
        totals[e["product_id"]] = totals.get(e["product_id"], 0) + e["delta"]
    for pid, stock in s.expected["stock_by_product"].items():
        assert stock == m.stock_total(pid) + totals[pid]
        assert stock >= 0
    ok = pd.DataFrame({"product_id": list(s.expected["stock_by_product"]), "stock": list(s.expected["stock_by_product"].values())})
    assert v.verify_rf03(s.expected, ok).ok
    bad = ok.copy()
    bad.loc[0, "stock"] += 1
    assert not v.verify_rf03(s.expected, bad).ok


def test_rf05_composition(m):
    s = sc.rf05(m)
    assert len(s.items) == 10_000
    flags = [i.record.flags for i in s.items]
    assert sum("duplicate" in f for f in flags) == 100
    assert sum(any(x.startswith("malformed") for x in f) for f in flags) == 100
    assert s.expected["silver_unique"] == 9_800 and s.expected["quarantine"] == 100
    silver = pd.DataFrame({"event_id": s.expected["silver_event_ids"]})
    assert v.verify_rf05(s.expected, silver, pd.DataFrame({"x": range(100)})).ok
    assert not v.verify_rf05(s.expected, pd.concat([silver, silver.head(1)]), pd.DataFrame({"x": range(100)})).ok


def test_rnf03_composition(m):
    s = sc.rnf03(m)
    assert len(s.items) == 10_100
    silver = pd.DataFrame({"event_id": list(s.expected["events"])})
    assert v.verify_rnf03(s.expected, pd.DataFrame({"a": range(10_100)}), silver).ok
    assert not v.verify_rnf03(s.expected, pd.DataFrame({"a": range(10_000)}), silver).ok


# ------------------------------------------------------------------ RF-07
def test_first_alert_oracle_hand_computed():
    # 1 unidad cada 12 s, stock 320: tras la venta k (1-based) cobertura = (320-k)*15/k < 60  <=>  k > 64  => k = 65
    offsets = [i * 12.0 for i in range(75)]
    idx = sc.first_alert_sale(offsets, [1] * 75, 320)
    assert idx == 64  # índice 0-based de la venta #65
    assert sc.first_alert_sale(offsets, [1] * 75, 100_000) is None  # stock enorme: nunca alerta


def test_rf07_scenario_expected(m):
    s = sc.rf07(m)
    assert s.realtime and s.expected["initial_stock"] == 320 and s.expected["trigger_index"] == 64
    assert s.expected["trigger_ts"] == v.parse_ts(s.expected["trigger_ts"]).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    trig = v.parse_ts(s.expected["trigger_ts"])
    ok = [{"product_id": "TEST-RF07", "received_at": (trig + timedelta(seconds=90)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"}]
    late = [{"product_id": "TEST-RF07", "received_at": (trig + timedelta(seconds=150)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"}]
    assert v.verify_rf07(s.expected, ok).ok
    assert not v.verify_rf07(s.expected, late).ok
    assert not v.verify_rf07(s.expected, []).ok


# ------------------------------------------------------------------ RF-08
def test_rf08_scenario_shape(m):
    s = sc.rf08(m, minutes=40)
    assert set(s.expected["present"]) == {"U-TEST-A"} and set(s.expected["absent"]) == {"U-TEST-B", "U-TEST-C"}
    a = s.expected["present"]["U-TEST-A"]
    assert a["value"] == 600_000
    purchases = [e for e in _events(s) if e["event_type"] == "purchase"]
    assert [e["user_id"] for e in purchases] == ["U-TEST-B"]
    b_purchase = next(i for i in s.items if i.record.event["event_type"] == "purchase")
    assert b_purchase.at == 20 * 60
    assert max(i.at for i in s.items) >= 40 * 60  # el latido cubre hasta el minuto 35 + margen
    # cada usuario del escenario existe en los maestros (enriquecimiento)
    assert {e["user_id"] for e in _events(s) if e["user_id"]} <= set(m.customers)


def test_rf08_verify_window(m):
    s = sc.rf08(m)
    a = s.expected["present"]["U-TEST-A"]
    lo, hi = v.parse_ts(a["earliest"]), v.parse_ts(a["deadline"])
    assert v.verify_rf08(s.expected, {"U-TEST-A": lo + timedelta(minutes=2), "U-TEST-B": None, "U-TEST-C": None}).ok
    assert not v.verify_rf08(s.expected, {"U-TEST-A": hi + timedelta(seconds=1), "U-TEST-B": None, "U-TEST-C": None}).ok  # tarde
    assert not v.verify_rf08(s.expected, {"U-TEST-A": lo - timedelta(minutes=1), "U-TEST-B": None, "U-TEST-C": None}).ok  # antes de los 30
    assert not v.verify_rf08(s.expected, {"U-TEST-A": lo, "U-TEST-B": lo, "U-TEST-C": None}).ok  # B no debía aparecer
    assert not v.verify_rf08(s.expected, {"U-TEST-A": None, "U-TEST-B": None, "U-TEST-C": None}).ok


def test_watch_dynamodb_records_first_seen(m):
    from datetime import datetime, timezone

    clock = {"t": datetime(2026, 1, 1, tzinfo=timezone.utc)}

    class Table:
        def get_item(self, Key, ConsistentRead):
            appeared = clock["t"] >= datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc) and Key["user_id"] == "A"
            return {"Item": {"user_id": "A"}} if appeared else {}

    def sleep(s):
        clock["t"] += timedelta(seconds=s)

    seen = v.watch_dynamodb_carts(Table(), ["A", "B"], datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), interval=10,
                                  now=lambda: clock["t"], sleep=sleep)
    assert seen["A"] == datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc) and seen["B"] is None


# ------------------------------------------------------------------ RF-09
def test_rf09_scenario_in_single_minute_and_verify(m):
    s = sc.rf09(m)
    stamps = {e["event_ts"][:16] for e in _events(s)}
    assert stamps == {s.expected["minute_start"][:16]} and s.expected["orders"] == 20
    assert s.expected["value"] == 12 * 100_000 + 8 * 50_000
    rows = pd.DataFrame([
        {"minute_start": s.expected["minute_start"], "category_id": None, "orders": 20, "value": 1_600_000.0},
        {"minute_start": s.expected["minute_start"], "category_id": 1, "orders": 12, "value": 1_200_000.0},
        {"minute_start": s.expected["minute_start"], "category_id": 3, "orders": 8, "value": 400_000.0}])
    deadline = v.parse_ts(s.expected["deadline"])
    assert v.verify_rf09(s.expected, rows, seen_at=deadline - timedelta(seconds=5)).ok
    assert not v.verify_rf09(s.expected, rows, seen_at=deadline + timedelta(seconds=5)).ok
    rows.loc[0, "orders"] = 19
    assert not v.verify_rf09(s.expected, rows).ok


# ------------------------------------------------------------------ RF-10 · RF-11 · RF-13 · RNF-06
def test_funnel_oracle_hand_computed(m):
    pid = next(p for p in m.products if p.startswith("P"))
    cat = m.products[pid]["category_id"]

    def ev(sess, t):
        return {"event_type": t, "product_id": pid, "device": "app", "city": "Cali", "event_ts": "2026-11-08T10:15:00.000Z", "session_id": sess}
    clicks = [ev(f"s{i}", "page_view") for i in range(10)] + [ev(f"s{i}", "add_to_cart") for i in range(4)] + [ev("s0", "purchase"), ev("s1", "purchase")]
    g = v.funnel_oracle(clicks, m)
    row = g.iloc[0]
    assert (row["category_id"], row["page_view"], row["add_to_cart"], row["purchase"]) == (cat, 10, 4, 2)
    assert row["view_to_cart"] == pytest.approx(0.4) and row["cart_to_purchase"] == pytest.approx(0.5)


def test_sales_oracle_ticket_is_sales_over_orders_not_mean_of_means(m):
    pid = next(p for p in m.products if p.startswith("P"))
    mk = lambda oid, q: {"event_type": "order_created", "order_id": oid, "city": "Cali", "department": "Valle", "event_ts": "2026-11-08T10:00:00.000Z",
                         "lines": [{"product_id": pid, "quantity": q, "unit_price": 1000.0, "discount": 0.0}]}
    g = v.sales_oracle([mk("a", 1), mk("b", 3)], m)
    assert g.iloc[0]["sales"] == 4000 and g.iloc[0]["orders"] == 2 and g.iloc[0]["avg_ticket"] == 2000


def test_compare_frames_detects_difference():
    a = pd.DataFrame({"k": [1, 2], "x": [1.0, 2.0]})
    assert v.compare_frames("t", a, a.copy(), ["k"], ["x"]).ok
    b = a.copy()
    b.loc[1, "x"] = 2.5
    assert not v.compare_frames("t", a, b, ["k"], ["x"]).ok
    assert not v.compare_frames("t", a, a.iloc[:1], ["k"], ["x"]).ok


def test_rf13_scenario(m):
    s = sc.rf13(m)
    orders = [e for e in _events(s) if e["event_type"] == "order_created"]
    ab = [o for o in orders if {l["product_id"] for l in o["lines"]} == {"TEST-RF13-A", "TEST-RF13-B"}]
    assert len(ab) == 50 and len(orders) == 100
    others = {}
    for o in orders:
        for l in o["lines"]:
            if l["product_id"] not in ("TEST-RF13-A", "TEST-RF13-B"):
                others[l["product_id"]] = others.get(l["product_id"], 0) + 1
    assert max(others.values()) == 10
    top = pd.DataFrame({"product_id": ["TEST-RF13-A"] * 2, "related_product_id": ["TEST-RF13-B", "P000001"], "rank": [1, 2]})
    assert v.verify_rf13(s.expected, top).ok
    assert not v.verify_rf13(s.expected, top.assign(rank=[2, 1])).ok


def test_rnf06_trace():
    gold = pd.DataFrame({"event_id": ["a", "b"]})
    assert v.verify_rnf06(gold, pd.DataFrame({"event_id": ["a", "b", "c"]})).ok
    assert not v.verify_rnf06(gold, pd.DataFrame({"event_id": ["a"]})).ok


def test_rf06_detects_wrong_enrichment(m):
    p = m.products[next(x for x in m.products if x.startswith("P"))]
    good = pd.DataFrame([{"event_id": "1", "product_id": p["product_id"], "category_id": p["category_id"], "seller_id": p["seller_id"]}])
    assert v.verify_rf06(m, good).ok
    assert not v.verify_rf06(m, good.assign(seller_id="S9999")).ok


# ------------------------------------------------------------------ reproducción
def test_play_fast_sends_everything(m, tmp_path):
    s = sc.rf09(m)
    out = FileSink(tmp_path)
    assert sc.play(s, out, realtime=False) == len(s.items)
    out.close()
    path = sc.write_expected(s, tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["expected"]["orders"] == 20


def test_play_realtime_respects_offsets(m, monkeypatch):
    s = sc.rf09(m, orders_a=2, orders_b=0)
    import time as t
    base = {"now": 1000.0}
    monkeypatch.setattr(sc.time, "time", lambda: base["now"])
    monkeypatch.setattr(sc, "_now", lambda: s.t0)
    sent_at = []

    class Sink(NullSink):
        def send(self, records):
            sent_at.extend([base["now"]] * len(records))

    def fake_sleep(d):
        base["now"] += d
    monkeypatch.setattr(sc.time, "sleep", fake_sleep)
    sc.play(s, Sink(), log=lambda *_: None)
    offsets = sorted(i.at for i in s.items)
    assert len(sent_at) == 2
    for sent, off in zip(sent_at, offsets):
        assert sent - 1000.0 >= off - 1e-6 and sent - 1000.0 < off + 0.5
