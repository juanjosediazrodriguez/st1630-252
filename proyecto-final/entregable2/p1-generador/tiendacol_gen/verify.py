"""Verificadores de los RF/RNF del E1 (columna "Cómo se verifica").

Son funciones puras: reciben lo esperado (`expected_<escenario>.json`) y los datos
observados como DataFrames/diccionarios, y devuelven un `Result`. Los cargadores
(`load_table`) leen CSV, JSONL, Parquet, Delta (si está `deltalake`) o Athena (si está
`pyathena`), de modo que las mismas funciones sirven en local y en AWS.

Nombres de columna: por defecto los del contrato (`event_id`, `product_id`, ...).
Si Gold/Silver usan otros, se pasa un `rename={"real": "esperado"}` al cargar.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd


@dataclass
class Result:
    name: str
    ok: bool
    details: dict = field(default_factory=dict)

    def __str__(self) -> str:
        flag = "PASA " if self.ok else "FALLA"
        return f"[{flag}] {self.name}: " + json.dumps(self.details, ensure_ascii=False, default=str)


def parse_ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _sample(items, n=5):
    items = list(items)
    return items[:n] + ([f"... (+{len(items) - n})"] if len(items) > n else [])


# ------------------------------------------------------------------ carga
def load_table(spec: str, rename: dict | None = None) -> pd.DataFrame:
    """spec: ruta .csv/.jsonl/.parquet, directorio Delta/Parquet, o `athena://<db>?<SQL>`."""
    if spec.startswith("athena://"):
        from pyathena import connect  # diferido
        db, _, sql = spec[len("athena://"):].partition("?")
        df = pd.read_sql(sql, connect(schema_name=db, region_name="us-east-1", s3_staging_dir=_staging()))
    else:
        p = Path(spec)
        if p.is_dir():
            if (p / "_delta_log").exists():
                from deltalake import DeltaTable  # diferido
                df = DeltaTable(str(p)).to_pandas()
            else:
                df = pd.read_parquet(p)
        elif p.suffix == ".csv":
            df = pd.read_csv(p)
        elif p.suffix == ".parquet":
            df = pd.read_parquet(p)
        else:  # jsonl; ignora líneas corruptas
            rows = []
            for line in p.read_bytes().splitlines():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            df = pd.DataFrame(rows)
    return df.rename(columns=rename) if rename else df


def _staging() -> str:
    import os
    return os.environ.get("ATHENA_STAGING", "s3://CAMBIAR-BUCKET-OPS/athena-results/")


def events_by_id(df: pd.DataFrame) -> dict[str, dict]:
    """Normaliza Bronze a {event_id: evento}. Acepta columnas planas o una columna JSON
    `payload`/`raw_payload`/`value` con el evento original."""
    for col in ("payload", "raw_payload", "value", "raw"):
        if col in df.columns:
            out = {}
            for v in df[col]:
                try:
                    ev = json.loads(v) if isinstance(v, (str, bytes)) else v
                except json.JSONDecodeError:
                    continue
                if isinstance(ev, dict) and ev.get("event_id"):
                    out[ev["event_id"]] = ev
            return out
    return {r["event_id"]: r for r in df.to_dict("records") if isinstance(r.get("event_id"), str)}


def _same(a, b, tol=1e-6) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    if isinstance(a, (int, float)) and isinstance(b, (int, float)) and not isinstance(a, bool):
        return abs(a - b) <= tol
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return all(_same(a[k], b.get(k)) for k in a)
    return a == b


# ------------------------------------------------------------------ RF-01 · RF-02 · RF-03 · RF-04
def verify_rf01(expected: dict, bronze: pd.DataFrame) -> Result:
    """Los N eventos enviados aparecen en Bronze con contenido idéntico."""
    got = events_by_id(bronze)
    missing = [e for e in expected["events"] if e not in got]
    diff = [e for e, ev in expected["events"].items() if e in got and not _same(ev, got[e])]
    ok = not missing and not diff
    return Result("RF-01 Bronze conserva los eventos sin modificar", ok,
                  {"esperados": expected["count"], "en_bronze": len(got), "faltan": _sample(missing), "distintos": _sample(diff)})


def verify_rf02(expected: dict, orders: pd.DataFrame) -> Result:
    """Bronze contiene las órdenes con sus líneas y pagos, y los cambios de estado."""
    got = events_by_id(orders)
    by_order = {ev["order_id"]: ev for ev in got.values() if ev.get("event_type") == "order_created"}
    bad = [o for o, e in expected["orders"].items()
           if o not in by_order or not _same(e["lines"], by_order[o].get("lines")) or not _same(e["payment"], by_order[o].get("payment"))]
    changes = [e for e in expected["status_change_event_ids"] if e not in got]
    return Result("RF-02 órdenes, líneas, pagos y cambios de estado", not bad and not changes,
                  {"ordenes_esperadas": len(expected["orders"]), "ordenes_con_problema": _sample(bad), "cambios_faltantes": len(changes)})


def verify_rf03(expected: dict, stock: pd.DataFrame, by: str = "product_id", col: str = "stock") -> Result:
    """El stock disponible reportado coincide con la fuente para todos los productos afectados."""
    got = dict(zip(stock[by], stock[col]))
    exp = expected["stock_by_product"]
    bad = {p: {"esperado": s, "obtenido": got.get(p)} for p, s in exp.items() if got.get(p) != s}
    return Result("RF-03 stock disponible", not bad, {"productos": len(exp), "discrepancias": bad})


def verify_rf04(manifest: dict, loaded_counts: dict[str, int]) -> Result:
    """A las 6:00 a. m. los conteos cargados coinciden con los archivos fuente del día."""
    bad = {k: {"fuente": v, "cargado": loaded_counts.get(k)} for k, v in manifest["counts"].items() if loaded_counts.get(k) != v}
    return Result("RF-04 maestros cargados", not bad, {"dia": manifest.get("day"), "discrepancias": bad})


def check_load_deadline(loaded_at: datetime, day: str, tz_offset_h: int = -5, hour: int = 6) -> Result:
    """La carga debe terminar antes de las 06:00 de Colombia (UTC-5) del día `day`."""
    deadline = datetime.fromisoformat(f"{day}T{hour:02d}:00:00").replace(tzinfo=timezone.utc) - pd.Timedelta(hours=tz_offset_h)
    return Result("RF-04 carga antes de las 06:00 COT", loaded_at <= deadline, {"cargado": loaded_at.isoformat(), "limite": deadline.isoformat()})


# ------------------------------------------------------------------ RF-05 · RF-06 · RNF-03
def verify_rf05(expected: dict, silver: pd.DataFrame, quarantine: pd.DataFrame) -> Result:
    ids = silver["event_id"]
    dup_in_silver = int(ids.duplicated().sum())
    missing = set(expected["silver_event_ids"]) - set(ids)
    ok = len(silver) == expected["silver_unique"] and len(quarantine) == expected["quarantine"] and not dup_in_silver and not missing
    return Result("RF-05 validación, cuarentena y deduplicación", ok,
                  {"silver": f"{len(silver)} (esperado {expected['silver_unique']})",
                   "cuarentena": f"{len(quarantine)} (esperado {expected['quarantine']})",
                   "duplicados_en_silver": dup_in_silver, "faltan_en_silver": _sample(missing)})


def verify_rnf03(expected: dict, bronze: pd.DataFrame, silver: pd.DataFrame) -> Result:
    """Bronze conserva todo lo recibido (incluidos reenvíos); Silver queda sin duplicados."""
    b = len(bronze)
    s_unique = silver["event_id"].nunique()
    ok = b == expected["bronze_count"] and len(silver) == expected["silver_unique"] and s_unique == len(silver)
    return Result("RNF-03 0 perdidos y 0 duplicados", ok,
                  {"bronze": f"{b} (esperado {expected['bronze_count']})", "silver": f"{len(silver)} (esperado {expected['silver_unique']})",
                   "silver_ids_unicos": s_unique})


def verify_rf06(masters, silver: pd.DataFrame, n: int = 1000, seed: int = 1) -> Result:
    """Muestra de n registros de Silver: categoría, vendedor, precio y ciudad coinciden con los maestros vigentes."""
    sample = silver.sample(min(n, len(silver)), random_state=seed)
    bad = []
    for r in sample.to_dict("records"):
        p = masters.products.get(r.get("product_id")) if r.get("product_id") else None
        if p:
            for col, key in (("category_id", "category_id"), ("seller_id", "seller_id")):
                if col in r and r[col] != p[key]:
                    bad.append((r.get("event_id"), col, r[col], p[key]))
        c = masters.customers.get(r.get("user_id")) if r.get("user_id") else None
        if c and "city" in r and (r["city"], r.get("department", c["department"])) != (c["city"], c["department"]):
            bad.append((r.get("event_id"), "city", r["city"], c["city"]))
    return Result("RF-06 enriquecimiento vs maestros", not bad, {"muestra": len(sample), "discrepancias": _sample(bad)})


# ------------------------------------------------------------------ RF-07 · RF-08 · RF-09 (tiempo real)
def verify_rf07(expected: dict, alerts: list[dict]) -> Result:
    """`alerts`: [{product_id, received_at(ISO), ...}] en orden de llegada a Operaciones."""
    mine = [a for a in alerts if a.get("product_id") == expected["product_id"]]
    if expected.get("trigger_ts") is None:
        return Result("RF-07 alerta de agotamiento", not mine, {"esperaba": "ninguna alerta", "recibidas": len(mine)})
    if not mine:
        return Result("RF-07 alerta de agotamiento", False, {"error": "no llegó ninguna alerta", "esperada_tras": expected["trigger_ts"]})
    first = min(parse_ts(a["received_at"]) for a in mine)
    lat = (first - parse_ts(expected["trigger_ts"])).total_seconds()
    ok = 0 <= lat <= expected["max_latency_s"]
    return Result("RF-07 alerta de agotamiento", ok,
                  {"latencia_s": round(lat, 1), "maximo_s": expected["max_latency_s"],
                   **({"aviso": "la alerta llegó ANTES de la venta esperada: revisar la fórmula de velocidad"} if lat < 0 else {}),
                   "alertas_duplicadas": len(mine) - 1})


def verify_rf08(expected: dict, first_seen: dict[str, datetime | None]) -> Result:
    """`first_seen[user_id]`: instante en que el usuario apareció en DynamoDB (None si nunca)."""
    problems = {}
    for u, info in expected["present"].items():
        seen = first_seen.get(u)
        if seen is None:
            problems[u] = "nunca apareció"
        else:
            lo, hi = parse_ts(info["earliest"]), parse_ts(info["deadline"])
            if not lo <= seen <= hi:
                problems[u] = f"apareció {seen.isoformat()} fuera de [{info['earliest']}, {info['deadline']}]"
    for u in expected["absent"]:
        if first_seen.get(u) is not None:
            problems[u] = "apareció pero no debía"
    return Result("RF-08 carritos abandonados", not problems, {"problemas": problems})


def _cat_key(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)) or str(x).lower() in ("total", "none", "nan", ""):
        return "total"
    try:
        return str(int(float(x)))
    except ValueError:
        return str(x)


def verify_rf09(expected: dict, rows: pd.DataFrame, seen_at: datetime | None = None) -> Result:
    """`rows`: filas de agg_sales_minute del minuto esperado con columnas
    minute_start, category_id (NULL o 'total' para el total), orders, value."""
    r = rows[rows["minute_start"].astype(str).str.startswith(expected["minute_start"][:16])].copy()
    r["_cat"] = r["category_id"].map(_cat_key)  # NULL/'total' -> 'total'; 1.0 -> '1' (Athena devuelve float si hay NULL)
    total = r[r["_cat"] == "total"]
    problems = {}
    if len(total) != 1 or int(total.iloc[0]["orders"]) != expected["orders"] or abs(float(total.iloc[0]["value"]) - expected["value"]) > 0.5:
        problems["total"] = {"esperado": [expected["orders"], expected["value"]],
                             "obtenido": total[["orders", "value"]].values.tolist()}
    for c, e in expected["by_category"].items():
        row = r[r["_cat"] == c]
        if len(row) != 1 or int(row.iloc[0]["orders"]) != e["orders"] or abs(float(row.iloc[0]["value"]) - e["value"]) > 0.5:
            problems[f"categoria_{c}"] = {"esperado": [e["orders"], e["value"]], "obtenido": row[["orders", "value"]].values.tolist()}
    det: dict = {"problemas": problems}
    ok = not problems
    if seen_at is not None:
        late = (seen_at - parse_ts(expected["deadline"])).total_seconds()
        det["segundos_sobre_el_limite"] = round(late, 1)
        ok = ok and late <= 0
    return Result("RF-09 ventas por minuto", ok, det)


def watch_dynamodb_carts(table, users: list[str], until: datetime, interval: float = 10.0, now=lambda: datetime.now(timezone.utc),
                         sleep=time.sleep, key: str = "user_id") -> dict[str, datetime | None]:
    """Consulta DynamoDB hasta `until` y registra cuándo apareció cada usuario (RF-08)."""
    seen: dict[str, datetime | None] = {u: None for u in users}
    while now() < until:
        for u in users:
            if seen[u] is None and table.get_item(Key={key: u}, ConsistentRead=True).get("Item"):
                seen[u] = now()
        sleep(interval)
    return seen


# ------------------------------------------------------------------ RF-10 · RF-11 · RF-13 · RNF-06
def funnel_oracle(clicks: list[dict], masters, hour_fmt: str = "%Y-%m-%dT%H") -> pd.DataFrame:
    """Sesiones distintas por etapa (page_view / add_to_cart / purchase) por categoría, dispositivo, ciudad y hora."""
    rows = []
    for e in clicks:
        if e.get("event_type") not in ("page_view", "add_to_cart", "purchase") or not e.get("product_id"):
            continue
        p = masters.products.get(e["product_id"])
        rows.append({"category_id": p["category_id"] if p else None, "device": e["device"], "city": e["city"],
                     "hour": e["event_ts"][:13], "stage": e["event_type"], "session_id": e["session_id"]})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    g = df.groupby(["category_id", "device", "city", "hour", "stage"])["session_id"].nunique().unstack("stage", fill_value=0).reset_index()
    for c in ("page_view", "add_to_cart", "purchase"):
        if c not in g:
            g[c] = 0
    g["view_to_cart"] = (g["add_to_cart"] / g["page_view"]).where(g["page_view"] > 0)
    g["cart_to_purchase"] = (g["purchase"] / g["add_to_cart"]).where(g["add_to_cart"] > 0)
    g["view_to_purchase"] = (g["purchase"] / g["page_view"]).where(g["page_view"] > 0)
    return g


def sales_oracle(orders: list[dict], masters) -> pd.DataFrame:
    """Ventas, órdenes y ticket promedio por categoría, vendedor, ciudad y fecha (ticket = ventas/órdenes)."""
    rows = []
    for e in orders:
        if e.get("event_type") != "order_created":
            continue
        for l in e["lines"]:
            p = masters.products[l["product_id"]]
            rows.append({"order_id": e["order_id"], "date": e["event_ts"][:10], "category_id": p["category_id"],
                         "seller_id": p["seller_id"], "city": e["city"], "department": e["department"],
                         "net": l["quantity"] * l["unit_price"] - l["discount"]})
    df = pd.DataFrame(rows)
    g = df.groupby(["date", "category_id", "seller_id", "city", "department"]).agg(
        sales=("net", "sum"), orders=("order_id", "nunique")).reset_index()
    g["avg_ticket"] = g["sales"] / g["orders"]
    return g


def compare_frames(name: str, expected: pd.DataFrame, got: pd.DataFrame, keys: list[str], cols: list[str], tol: float = 1e-6) -> Result:
    """Compara dos tablas por llave: mismas filas y mismos valores en `cols` (RF-10/RF-11: tablero = consulta = oráculo)."""
    m = expected.merge(got, on=keys, how="outer", suffixes=("_esp", "_obt"), indicator=True)
    only_exp, only_got = m[m["_merge"] == "left_only"], m[m["_merge"] == "right_only"]
    both = m[m["_merge"] == "both"]
    bad = both[[any(not _same(r[f"{c}_esp"], r[f"{c}_obt"], tol) for c in cols) for r in both.to_dict("records")]]
    return Result(name, only_exp.empty and only_got.empty and bad.empty,
                  {"filas_esperadas": len(expected), "filas_obtenidas": len(got), "faltan": len(only_exp),
                   "sobran": len(only_got), "valores_distintos": len(bad), "ejemplo": bad.head(3).to_dict("records")})


def verify_rf13(expected: dict, top: pd.DataFrame, product_col="product_id", other_col="related_product_id", rank_col="rank") -> Result:
    """B es el primero de la lista de A y ningún otro producto supera lo que indica el escenario."""
    e = expected["top_bought_together"]
    t = top[top[product_col] == e["product"]].sort_values(rank_col)
    first = t.iloc[0][other_col] if len(t) else None
    return Result("RF-13 productos comprados/vistos juntos", first == e["first"], {"primero_obtenido": first, "esperado": e["first"]})


def verify_rnf06(gold: pd.DataFrame, bronze: pd.DataFrame, gold_key: str = "event_id", bronze_key: str = "event_id") -> Result:
    """100 % de las filas de Gold enlazan con Bronze (consulta de cruce)."""
    missing = set(gold[gold_key].dropna()) - set(bronze[bronze_key].dropna())
    return Result("RNF-06 trazabilidad Gold → Bronze", not missing, {"filas_gold": len(gold), "sin_origen": len(missing), "ejemplo": _sample(missing)})
