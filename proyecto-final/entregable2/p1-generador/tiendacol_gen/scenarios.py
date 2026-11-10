"""Escenarios de prueba de la columna "Cómo se verifica" del E1.

Cada escenario genera los eventos de entrada y el diccionario `expected` con lo que
debe observarse aguas abajo. `verify.py` compara `expected` contra Bronze/Silver/Gold,
DynamoDB o las alertas. Los de tiempo real (RF-07, RF-08, RF-09) se reproducen en
tiempo real porque dependen del watermark y de las ventanas de Spark.
"""
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .events import CLICKSTREAM, ORDERS, STOCK, EventFactory, Record, iso
from .masters import FIXTURE_WAREHOUSE, Masters
from .sinks import Sink


@dataclass
class Scheduled:
    at: float          # segundos desde t0
    record: Record


@dataclass
class Scenario:
    name: str
    t0: datetime
    items: list[Scheduled]
    expected: dict
    realtime: bool = False   # True: hay que reproducirlo en tiempo real (ventanas / watermark)


def _factory(m: Masters, seed: int) -> EventFactory:
    return EventFactory(m, seed, dup_rate=0, late_rate=0, malformed_rate=0)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _unique_clicks(f: EventFactory, n: int, ts: datetime) -> list[Record]:
    out = []
    types = ["page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start"]
    for i in range(n):
        s = f._new_session()
        p = f._pick_product()
        t = types[i % len(types)]
        extra: dict = {"page_view": {"product_id": p["product_id"]}, "search": {"query": "tenis"},
                       "add_to_cart": {"product_id": p["product_id"], "quantity": 1, "unit_price": float(p["price"])},
                       "remove_from_cart": {"product_id": p["product_id"], "quantity": 1},
                       "checkout_start": {"cart_value": float(p["price"])}}[t]
        out.append(f.click(s, t, ts, **extra))
    return out


def _sched(records: list[Record], at: float = 0.0) -> list[Scheduled]:
    return [Scheduled(at, r) for r in records]


def _event_index(records: list[Record]) -> dict[str, dict]:
    return {r.event_id: r.event for r in records if r.event}


# ---------------------------------------------------------------- RF-01 / RNF-03 / RF-05
def rf01(m: Masters, n: int = 10_000, seed: int = 101) -> Scenario:
    """10.000 eventos únicos: deben aparecer idénticos en Bronze."""
    f, t0 = _factory(m, seed), _now()
    recs = _unique_clicks(f, n, t0)
    return Scenario("rf01", t0, _sched(recs), {"count": n, "events": _event_index(recs)})


def rnf03(m: Masters, n: int = 10_000, resent: int = 100, seed: int = 103) -> Scenario:
    """n eventos y `resent` reenviados: Bronze conserva n+resent, Silver n únicos."""
    f, t0 = _factory(m, seed), _now()
    recs = _unique_clicks(f, n, t0)
    rng = random.Random(seed)
    dups = [Record(r.stream_type, r.key, r.event, r.payload(), ("duplicate",)) for r in rng.sample(recs, resent)]
    items = _sched(recs + dups)
    rng.shuffle(items)
    return Scenario("rnf03", t0, items, {"bronze_count": n + resent, "silver_unique": n, "events": _event_index(recs)})


def rf05(m: Masters, total: int = 10_000, malformed: int = 100, duplicates: int = 100, seed: int = 105) -> Scenario:
    """`total` eventos = (total-malformed-duplicates) válidos únicos + duplicados + malformados."""
    f, t0 = _factory(m, seed), _now()
    rng = random.Random(seed)
    unique = total - malformed - duplicates
    valid = _unique_clicks(f, unique, t0)
    dups = [Record(r.stream_type, r.key, r.event, r.payload(), ("duplicate",)) for r in rng.sample(valid, duplicates)]
    bad = [f._malform(r) for r in _unique_clicks(f, malformed, t0)]
    items = _sched(valid + dups + bad)
    rng.shuffle(items)
    return Scenario("rf05", t0, items, {
        "sent": total, "silver_unique": unique, "quarantine": malformed,
        "silver_event_ids": sorted(r.event_id for r in valid),
        "malformed_causes": {c: sum(1 for r in bad if r.flags[0] == f"malformed:{c}")
                             for c in ("missing_field", "wrong_type", "unknown_type", "truncated_json")}})


# ---------------------------------------------------------------- RF-02
def rf02(m: Masters, orders: int = 100, changed: int = 20, seed: int = 102) -> Scenario:
    """100 órdenes con líneas y pago; 20 cambian de estado de envío."""
    f, t0 = _factory(m, seed), _now()
    rng = random.Random(seed)
    recs, exp = [], {}
    for i in range(orders):
        s = f._new_session()
        lines = []
        for p in rng.sample(f._products, k=rng.randint(1, 3)):
            lines.append({"product_id": p, "quantity": rng.randint(1, 3),
                          "unit_price": float(m.products[p]["price"]), "discount": 0.0})
        rec = f.order_created(f"O-RF02-{i:04d}", s.user_id, s.city, s.department, lines, t0)
        recs.append(rec)
        exp[rec.event["order_id"]] = {"lines": lines, "payment": rec.event["payment"], "total": rec.event["total"]}
    changes = [f.status_changed(r.event["order_id"], "paid", "packed", t0 + timedelta(seconds=1)) for r in rng.sample(recs, changed)]
    return Scenario("rf02", t0, _sched(recs) + [Scheduled(1.0, c) for c in changes],
                    {"orders": exp, "status_change_event_ids": sorted(c.event_id for c in changes)})


# ---------------------------------------------------------------- RF-03
def rf03(m: Masters, changes: int = 50, seed: int = 103) -> Scenario:
    """50 cambios de stock sobre 5 productos; el stock final se calcula desde el inventario maestro."""
    f, t0 = _factory(m, seed), _now()
    rng = random.Random(seed)
    keys = rng.sample([k for k in m.inventory if not k[0].startswith("TEST-")], k=5)
    stock = {k: m.inventory[k] for k in keys}
    recs = []
    for i in range(changes):
        pid, wh = rng.choice(keys)
        delta = rng.choice([-1, -2, -3, -5, 10, 25])
        if stock[(pid, wh)] + delta < 0:
            delta = abs(delta)
        stock[(pid, wh)] += delta
        recs.append(f.stock_change(pid, wh, delta, "sale" if delta < 0 else "restock", t0 + timedelta(milliseconds=i)))
    touched = {pid for pid, _ in keys}
    by_product = {pid: sum(stock.get(k, v) for k, v in m.inventory.items() if k[0] == pid) for pid in touched}
    return Scenario("rf03", t0, _sched(recs, 0.0),
                    {"stock_by_product_warehouse": {f"{p}|{w}": s for (p, w), s in stock.items()},
                     "stock_by_product": by_product, "event_ids": [r.event_id for r in recs]})


# ---------------------------------------------------------------- RF-07
def first_alert_sale(sale_offsets: list[float], units: list[int], stock0: int, window_s: float = 900.0,
                     horizon_min: float = 60.0) -> int | None:
    """Oráculo de RF-07. Devuelve el índice de la primera venta tras la cual el stock alcanza
    para menos de `horizon_min` minutos a la velocidad de los últimos `window_s` segundos.

    velocidad (u/min) = unidades vendidas en la ventana / (window_s/60); cobertura = stock / velocidad.
    """
    stock, lo, in_window = stock0, 0, 0
    for k, (t, u) in enumerate(zip(sale_offsets, units)):
        stock -= u
        in_window += u
        while sale_offsets[lo] <= t - window_s:
            in_window -= units[lo]
            lo += 1
        velocity = in_window / (window_s / 60)
        if velocity > 0 and stock / velocity < horizon_min:
            return k
    return None


def rf07(m: Masters, sale_interval: float = 12.0, minutes: float = 15.0, seed: int = 107) -> Scenario:
    """Ventas de 1 unidad cada `sale_interval` s sobre TEST-RF07 (stock inicial conocido).
    Con los valores por defecto la alerta debe salir tras la venta #65 (≈ 13 min)."""
    f, t0 = _factory(m, seed), _now()
    pid, stock0 = "TEST-RF07", m.stock_total("TEST-RF07")
    price = float(m.products[pid]["price"])
    n = int(minutes * 60 / sale_interval)
    items, offsets, order_ids = [], [], []
    s = f._new_session()
    for k in range(n):
        at = k * sale_interval
        ts = t0 + timedelta(seconds=at)
        oid = f"O-RF07-{k:04d}"
        order = f.order_created(oid, s.user_id, s.city, s.department,
                                [{"product_id": pid, "quantity": 1, "unit_price": price, "discount": 0.0}], ts)
        items += [Scheduled(at, order), Scheduled(at, f.stock_change(pid, FIXTURE_WAREHOUSE, -1, "sale", ts))]
        offsets.append(at)
        order_ids.append(order.event_id)
    idx = first_alert_sale(offsets, [1] * n, stock0)
    exp = {"product_id": pid, "initial_stock": stock0, "sales": n, "trigger_index": idx, "max_latency_s": 120}
    if idx is not None:
        exp.update(trigger_event_id=order_ids[idx], trigger_ts=iso(t0 + timedelta(seconds=offsets[idx])),
                   stock_at_trigger=stock0 - (idx + 1))
    return Scenario("rf07", t0, items, exp, realtime=True)


# ---------------------------------------------------------------- RF-08
def rf08(m: Masters, heartbeat_s: float = 10.0, minutes: float = 40.0, seed: int = 108) -> Scenario:
    """Tres sesiones de E1 (RF-08) más tráfico de fondo.

    A: carrito de COP 600.000 sin compra  -> debe aparecer entre el min 30 y 35 tras su último evento.
    B: carrito de COP 600.000, compra en el min 20 -> no debe aparecer.
    C: carrito de COP 300.000 sin compra -> no debe aparecer.
    El latido (page_view cada `heartbeat_s`) es necesario: el watermark solo avanza con eventos nuevos (E1, ADR-2).
    """
    f, t0 = _factory(m, seed), _now()
    from .events import _Session
    pid = "TEST-RF09-A"  # COP 100.000 la unidad

    def sess(sid, user):
        c = m.customers[user]
        return _Session(sid=sid, user_id=user, device="app", city=c["city"], department=c["department"])

    items: list[Scheduled] = []
    expected: dict = {"present": {}, "absent": {}, "window_min": [30, 35]}

    def at(sec, rec):
        items.append(Scheduled(sec, rec))

    for tag, user, qty, buys in (("A", "U-TEST-A", 6, False), ("B", "U-TEST-B", 6, True), ("C", "U-TEST-C", 3, False)):
        s = sess(f"s-rf08-{tag}", user)
        ts = lambda sec: t0 + timedelta(seconds=sec)
        at(0, f.click(s, "page_view", ts(0), product_id=pid))
        at(10, f.click(s, "add_to_cart", ts(10), product_id=pid, quantity=qty, unit_price=100_000.0))
        last = 10
        if buys:
            last = 20 * 60
            at(last, f.click(s, "purchase", ts(last), cart_value=qty * 100_000.0, order_id="O-RF08-B"))
            at(last, f.order_created("O-RF08-B", user, s.city, s.department,
                                     [{"product_id": pid, "quantity": qty, "unit_price": 100_000.0, "discount": 0.0}], ts(last)))
        info = {"user_id": user, "value": qty * 100_000.0, "last_event_ts": iso(ts(last))}
        if tag == "A":
            info.update(earliest=iso(ts(last + 30 * 60)), deadline=iso(ts(last + 35 * 60)))
            expected["present"][user] = info
        else:
            expected["absent"][user] = info
    noise = sess("s-rf08-noise", "U-TEST-NOISE")
    k = 0
    while k * heartbeat_s <= minutes * 60:
        sec = k * heartbeat_s
        at(sec, f.click(noise, "page_view", t0 + timedelta(seconds=sec), product_id=pid))
        k += 1
    return Scenario("rf08", t0, items, expected, realtime=True)


# ---------------------------------------------------------------- RF-09
def rf09(m: Masters, orders_a: int = 12, orders_b: int = 8, seed: int = 109) -> Scenario:
    """Compras conocidas dentro de un mismo minuto calendario (el siguiente al lanzar)."""
    f = _factory(m, seed)
    now = _now()
    minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    t0 = minute - timedelta(seconds=3)  # arranca 3 s antes para que el aviso al operador sea previo
    s = f._new_session()
    items = []
    specs = [("TEST-RF09-A", 100_000.0)] * orders_a + [("TEST-RF09-B", 50_000.0)] * orders_b
    random.Random(seed).shuffle(specs)
    for i, (pid, price) in enumerate(specs):
        off = 3 + (i * 50.0 / len(specs))  # reparte dentro de los primeros 53 s del minuto
        ts = t0 + timedelta(seconds=off)
        items.append(Scheduled(off, f.order_created(f"O-RF09-{seed}-{i:03d}", s.user_id, s.city, s.department,
                                                     [{"product_id": pid, "quantity": 1, "unit_price": price, "discount": 0.0}], ts)))
    cats = {m.products[p]["category_id"]: 0 for p, _ in specs}
    vals = dict.fromkeys(cats, 0.0)
    for p, price in specs:
        c = m.products[p]["category_id"]
        cats[c] += 1
        vals[c] += price
    exp = {"minute_start": iso(minute), "orders": len(specs), "value": sum(p for _, p in specs),
           "by_category": {str(c): {"orders": cats[c], "value": vals[c]} for c in cats},
           "deadline": iso(minute + timedelta(minutes=1, seconds=120)), "max_latency_s": 120}
    return Scenario("rf09", t0, items, exp, realtime=True)


# ---------------------------------------------------------------- RF-13
def rf13(m: Masters, together: int = 50, others: int = 5, per_other: int = 10, seed: int = 113) -> Scenario:
    """A y B juntos en 50 órdenes y 50 sesiones; otros 5 productos con A en 10 cada uno."""
    f, t0 = _factory(m, seed), _now()
    a, b = "TEST-RF13-A", "TEST-RF13-B"
    rivals = [p for p in f._products[:others]]
    price = lambda p: float(m.products[p]["price"])
    items = []

    def pair(i, other):
        s = f._new_session()
        oid = f"O-RF13-{i:04d}"
        lines = [{"product_id": p, "quantity": 1, "unit_price": price(p), "discount": 0.0} for p in (a, other)]
        items.append(Scheduled(0, f.order_created(oid, s.user_id, s.city, s.department, lines, t0)))
        for p in (a, other):
            items.append(Scheduled(0, f.click(s, "page_view", t0, product_id=p)))

    i = 0
    for _ in range(together):
        pair(i, b)
        i += 1
    for r in rivals:
        for _ in range(per_other):
            pair(i, r)
            i += 1
    exp_orders = {"product": a, "first": b, "first_count": together, "others": {r: per_other for r in rivals}}
    return Scenario("rf13", t0, items, {"top_bought_together": exp_orders, "top_viewed_together": exp_orders})


SCENARIOS = {"rf01": rf01, "rf02": rf02, "rf03": rf03, "rf05": rf05, "rnf03": rnf03,
             "rf07": rf07, "rf08": rf08, "rf09": rf09, "rf13": rf13}


# ---------------------------------------------------------------- reproducción
def play(sc: Scenario, sink: Sink, realtime: bool | None = None, log=print) -> int:
    """Envía el escenario. En tiempo real espera a t0+at antes de enviar cada registro."""
    realtime = sc.realtime if realtime is None else realtime
    items = sorted(sc.items, key=lambda s: s.at)
    i, sent = 0, 0
    start = time.time() + max(0.0, (sc.t0 - _now()).total_seconds())
    last_log = 0.0
    while i < len(items):
        now = time.time()
        horizon = (now - start) if realtime else float("inf")
        batch = []
        while i < len(items) and items[i].at <= horizon and len(batch) < 2000:
            batch.append(items[i].record)
            i += 1
        if batch:
            sink.send(batch)
            sent += len(batch)
        elif realtime:
            time.sleep(min(0.2, max(0.01, items[i].at - horizon)))
        if realtime and now - last_log > 60:
            log(f"[{sc.name}] t+{now - start:5.0f}s  enviados {sent}/{len(items)}")
            last_log = now
    return sent


def write_expected(sc: Scenario, directory: str | Path) -> Path:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"expected_{sc.name}.json"
    path.write_text(json.dumps({"scenario": sc.name, "t0": iso(sc.t0), "sent": len(sc.items), "expected": sc.expected},
                               ensure_ascii=False, indent=1), encoding="utf-8")
    return path
