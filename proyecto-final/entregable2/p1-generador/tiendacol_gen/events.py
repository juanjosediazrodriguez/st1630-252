"""Fábrica de eventos: sesiones de navegación coherentes con órdenes y stock.

Cada sesión recorre el embudo ver producto -> carrito -> checkout -> compra con
probabilidades que reproducen ~70 % de carritos abandonados (E1, sección 1.2).
Una compra emite además la orden (stream `orders`) y un cambio de stock por línea
(stream `stock_changes`), de modo que las tres fuentes son consistentes entre sí.

Mutaciones (duplicados, tardíos, malformados) se aplican en `EventFactory.next_batch`
y quedan contadas en `stats` para poder comparar con Bronze/Silver.
"""
from __future__ import annotations

import heapq
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .masters import CITIES, Masters, WAREHOUSES

CLICKSTREAM, ORDERS, STOCK = "clickstream", "orders", "stock_changes"
STREAM_TYPES = (CLICKSTREAM, ORDERS, STOCK)

DEVICES = ["desktop", "mobile_web", "app"]
DEVICE_WEIGHTS = [30, 30, 40]
SEARCH_TERMS = ["audífonos", "tenis", "licuadora", "perfume", "bicicleta", "celular", "jean", "lámpara", "balón", "serum"]
PAY_METHODS = ["card", "pse", "nequi", "cash_on_delivery"]
STATUS_FLOW = {"paid": "packed", "packed": "shipped", "shipped": "delivered"}


def iso(ts: datetime) -> str:
    """Marca de tiempo UTC con milisegundos y sufijo Z (formato del contrato)."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass
class Record:
    stream_type: str
    key: str                      # clave de partición de Kinesis
    event: dict | None            # None cuando el payload es JSON corrupto (solo `raw`)
    raw: bytes | None = None      # payload serializado (se calcula perezosamente)
    flags: tuple[str, ...] = ()   # "duplicate", "late", "malformed:<causa>"

    def payload(self) -> bytes:
        if self.raw is None:
            self.raw = json.dumps(self.event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return self.raw

    @property
    def event_id(self) -> str | None:
        return (self.event or {}).get("event_id")


@dataclass
class _Session:
    sid: str
    user_id: str | None
    device: str
    city: str
    department: str
    state: str = "start"
    cart: dict[str, dict] = field(default_factory=dict)  # product_id -> {quantity, unit_price}
    viewed: dict | None = None  # último producto visto

    def cart_value(self) -> float:
        return float(sum(i["quantity"] * i["unit_price"] for i in self.cart.values()))


@dataclass
class Stats:
    by_stream: dict[str, int] = field(default_factory=lambda: {s: 0 for s in STREAM_TYPES})
    logical: int = 0      # eventos distintos generados (sin copias duplicadas)
    duplicates: int = 0
    late: int = 0
    malformed: int = 0

    @property
    def emitted(self) -> int:
        return sum(self.by_stream.values())


class EventFactory:
    def __init__(self, masters: Masters, seed: int = 1, *, identified_ratio: float = 0.65,
                 dup_rate: float = 0.01, late_rate: float = 0.01, late_seconds: float = 120.0,
                 malformed_rate: float = 0.01, dup_max_delay: float = 20.0,
                 active_sessions: int = 2000, status_change_rate: float = 0.05, restock_rate: float = 0.002,
                 hot_products: int = 0):
        self.m = masters
        self.rng = random.Random(seed)
        self.identified_ratio = identified_ratio
        self.dup_rate, self.late_rate, self.malformed_rate = dup_rate, late_rate, malformed_rate
        self.late_seconds, self.dup_max_delay = late_seconds, dup_max_delay
        self.target_active = active_sessions
        self.status_change_rate, self.restock_rate = status_change_rate, restock_rate
        self._products = [p for p in masters.products if not p.startswith("TEST-")]
        self._customers = list(masters.customers.values())
        # Un puñado de productos concentra la demanda (los picos de Black Friday); 0 = uniforme.
        self._hot = self._products[:hot_products] if hot_products else []
        self._wh = {}
        for (pid, wh) in masters.inventory:
            self._wh.setdefault(pid, []).append(wh)
        self._sessions: list[_Session] = []
        self._open_orders: list[tuple[str, str]] = []  # (order_id, status)
        self._delayed: list[tuple[float, int, Record]] = []
        self._seq = 0
        self._order_seq = 0
        self.stats = Stats()

    # -- construcción de eventos ------------------------------------------------
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))  # determinista dada la semilla

    def _base(self, etype: str, ts: datetime) -> dict:
        return {"event_id": self._uuid(), "event_type": etype, "event_ts": iso(ts), "schema_version": "1.0"}

    def click(self, s: _Session, etype: str, ts: datetime, **extra) -> Record:
        ev = {**self._base(etype, ts), "session_id": s.sid, "user_id": s.user_id, "device": s.device,
              "city": s.city, "department": s.department, "product_id": None, "query": None,
              "quantity": None, "unit_price": None, "cart_value": None, "order_id": None, **extra}
        return Record(CLICKSTREAM, s.user_id or s.sid, ev)

    def order_created(self, order_id: str, user_id: str | None, city: str, dept: str, lines: list[dict],
                      ts: datetime) -> Record:
        total = float(sum(l["quantity"] * l["unit_price"] - l["discount"] for l in lines))
        ev = {**self._base("order_created", ts), "order_id": order_id, "user_id": user_id, "city": city,
              "department": dept, "status": "paid", "lines": lines, "total": total,
              "payment": {"method": self.rng.choice(PAY_METHODS), "amount": total, "status": "approved"}}
        return Record(ORDERS, order_id, ev)

    def status_changed(self, order_id: str, prev: str, new: str, ts: datetime) -> Record:
        ev = {**self._base("order_status_changed", ts), "order_id": order_id, "status": new, "previous_status": prev}
        return Record(ORDERS, order_id, ev)

    def stock_change(self, pid: str, wh: str, delta: int, reason: str, ts: datetime) -> Record:
        ev = {**self._base("stock_change", ts), "product_id": pid, "warehouse_id": wh, "delta": delta, "reason": reason}
        return Record(STOCK, pid, ev)

    # -- simulación de sesiones -------------------------------------------------
    def _new_session(self) -> _Session:
        rng = self.rng
        if rng.random() < self.identified_ratio and self._customers:
            c = rng.choice(self._customers)
            user, city, dept = c["customer_id"], c["city"], c["department"]
        else:
            city, dept, _ = rng.choices(CITIES, weights=[w for *_, w in CITIES])[0]
            user = None
        return _Session(sid="s-" + self._uuid()[:13], user_id=user, device=rng.choices(DEVICES, DEVICE_WEIGHTS)[0],
                        city=city, department=dept)

    def _pick_product(self) -> dict:
        pool = self._hot if self._hot and self.rng.random() < 0.5 else self._products
        return self.m.products[self.rng.choice(pool)]

    def _step(self, s: _Session, ts: datetime) -> tuple[list[Record], bool]:
        """Avanza una sesión un paso. Devuelve (registros, sesión_terminada)."""
        rng, out = self.rng, []
        if s.state in ("start", "browsing"):
            if s.state == "start":
                s.state = "browsing"
            r = rng.random()
            if r < 0.18 and s.viewed:
                p = s.viewed
                qty = rng.choice([1, 1, 1, 2, 3])
                cur = s.cart.get(p["product_id"], {"quantity": 0, "unit_price": p["price"]})
                cur["quantity"] += qty
                s.cart[p["product_id"]] = cur
                out.append(self.click(s, "add_to_cart", ts, product_id=p["product_id"], quantity=qty,
                                      unit_price=float(p["price"])))
                s.state = "carting"
            elif r < 0.25:
                out.append(self.click(s, "search", ts, query=rng.choice(SEARCH_TERMS)))
            elif r < 0.30:
                return [], True  # rebote
            else:
                p = self._pick_product()
                s.viewed = p
                out.append(self.click(s, "page_view", ts, product_id=p["product_id"]))
        elif s.state == "carting":
            r = rng.random()
            if r < 0.10 and s.cart:
                pid = rng.choice(list(s.cart))
                out.append(self.click(s, "remove_from_cart", ts, product_id=pid, quantity=1))
                s.cart[pid]["quantity"] -= 1
                if s.cart[pid]["quantity"] <= 0:
                    del s.cart[pid]
                if not s.cart:
                    s.state = "browsing"
            elif r < 0.40:
                s.state = "browsing"
            elif r < 0.70 and s.cart:
                out.append(self.click(s, "checkout_start", ts, cart_value=s.cart_value()))
                s.state = "checkout"
            else:
                return [], True  # carrito abandonado
        elif s.state == "checkout":
            if rng.random() < 0.55:
                self._order_seq += 1
                oid = f"O{self.rng.getrandbits(40):010x}{self._order_seq % 100:02d}"
                lines = [{"product_id": pid, "quantity": i["quantity"], "unit_price": float(i["unit_price"]), "discount": 0.0}
                         for pid, i in s.cart.items()]
                out.append(self.click(s, "purchase", ts, cart_value=s.cart_value(), order_id=oid))
                out.append(self.order_created(oid, s.user_id, s.city, s.department, lines, ts))
                for l in lines:
                    wh = rng.choice(self._wh.get(l["product_id"], WAREHOUSES[:1]))
                    out.append(self.stock_change(l["product_id"], wh, -l["quantity"], "sale", ts))
                self._open_orders.append((oid, "paid"))
            return out, True
        return out, False

    def _background(self, ts: datetime) -> list[Record]:
        """Cambios de estado de envío y reposición de stock."""
        out, rng = [], self.rng
        if self._open_orders and rng.random() < self.status_change_rate:
            i = rng.randrange(len(self._open_orders))
            oid, st = self._open_orders[i]
            new = STATUS_FLOW[st]
            out.append(self.status_changed(oid, st, new, ts))
            if new == "delivered":
                self._open_orders[i] = self._open_orders[-1]
                self._open_orders.pop()
            else:
                self._open_orders[i] = (oid, new)
        if rng.random() < self.restock_rate:
            pid = rng.choice(self._products)
            out.append(self.stock_change(pid, rng.choice(self._wh.get(pid, WAREHOUSES[:1])), rng.randint(20, 120), "restock", ts))
        return out

    # -- mutaciones -------------------------------------------------------------
    def _malform(self, rec: Record) -> Record:
        """Corrompe un registro. Causas: campo requerido ausente, tipo erróneo, tipo de evento
        desconocido o JSON truncado. Un registro corrupto nunca es un duplicado ni se reenvía."""
        rng = self.rng
        cause = rng.choice(["missing_field", "wrong_type", "unknown_type", "truncated_json"])
        ev = json.loads(json.dumps(rec.event))
        if cause == "missing_field":
            ev.pop(rng.choice(["event_id", "event_ts", "event_type", "session_id" if rec.stream_type == CLICKSTREAM else "order_id"]), None)
            if rec.stream_type == STOCK:
                ev.pop("delta", None)
        elif cause == "wrong_type":
            ev["event_ts"] = "ayer en la tarde"
        elif cause == "unknown_type":
            ev["event_type"] = "teleport"
        else:
            raw = json.dumps(ev).encode("utf-8")
            return Record(rec.stream_type, rec.key, None, raw[: len(raw) // 2], ("malformed:truncated_json",))
        return Record(rec.stream_type, rec.key, ev, None, (f"malformed:{cause}",))

    def _apply_mutations(self, rec: Record, now: datetime, now_s: float) -> list[Record]:
        rng, out = self.rng, []
        if rng.random() < self.malformed_rate:
            self.stats.malformed += 1
            return [self._malform(rec)]
        if rng.random() < self.late_rate and rec.event:
            ts = now - timedelta(seconds=self.late_seconds * rng.uniform(1.0, 1.5))
            rec.event["event_ts"] = iso(ts)
            rec.flags += ("late",)
            self.stats.late += 1
        self.stats.logical += 1
        out.append(rec)
        if rng.random() < self.dup_rate:
            dup = Record(rec.stream_type, rec.key, rec.event, rec.payload(), rec.flags + ("duplicate",))
            self.stats.duplicates += 1
            self._seq += 1
            heapq.heappush(self._delayed, (now_s + rng.uniform(0, self.dup_max_delay), self._seq, dup))
        return out

    # -- API pública ------------------------------------------------------------
    def next_batch(self, n: int, now: datetime | None = None, now_s: float = 0.0) -> list[Record]:
        """Genera ~n registros con event_ts = `now` (UTC actual por defecto)."""
        now = now or datetime.now(timezone.utc)
        out: list[Record] = []
        while self._delayed and self._delayed[0][0] <= now_s:  # duplicados cuyo retraso venció
            out.append(heapq.heappop(self._delayed)[2])
        while len(out) < n:
            while len(self._sessions) < self.target_active:
                self._sessions.append(self._new_session())
            i = self.rng.randrange(len(self._sessions))
            recs, done = self._step(self._sessions[i], now)
            if done:
                self._sessions[i] = self._sessions[-1]
                self._sessions.pop()
            for r in recs + self._background(now):
                out.extend(self._apply_mutations(r, now, now_s))
        for r in out:
            self.stats.by_stream[r.stream_type] += 1
        return out
