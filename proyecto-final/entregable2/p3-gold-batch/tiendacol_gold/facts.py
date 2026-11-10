"""Construcción de las tablas de hechos (`fact_funnel_event`, `fact_sales`) y de afinidad (`agg_product_affinity`).

Lee tanto eventos de Silver (el esquema `SILVER_SCHEMA` de P2) como diccionarios de los
escenarios de P1, garantizando compatibilidad total con `verify.py` y `datasets.sql`.
"""
from __future__ import annotations

import json
from functools import lru_cache
import pandas as pd

from .dimensions import (
    apply_scd2_products_df,
    build_dim_customer_df,
    build_dim_datetime_df,
    build_dim_device_df,
    build_dim_location_df,
    build_dim_seller_df,
    resolve_product_version,
)
from .tables import surrogate_key

FUNNEL_STAGES = ("page_view", "add_to_cart", "purchase")


def _norm_ts_str(ts) -> str:
    """Normaliza cualquier timestamp (datetime, pd.Timestamp o str) a ISO-8601 UTC."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "2026-01-01T00:00:00.000Z"
    if isinstance(ts, str):
        return ts.replace(" ", "T")
    if hasattr(ts, "strftime"):
        return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{getattr(ts, 'microsecond', 0) // 1000:03d}Z"
    return str(ts)


def _parse_lines(lines_val) -> list[dict]:
    if lines_val is None:
        return []
    if isinstance(lines_val, str):
        try:
            return json.loads(lines_val)
        except Exception:  # noqa: BLE001
            return []
    if isinstance(lines_val, list):
        out = []
        for item in lines_val:
            if isinstance(item, dict):
                out.append(item)
            elif hasattr(item, "asDict"):
                out.append(item.asDict())
        return out
    return list(lines_val)


def build_fact_sales_df(
    orders_events: list[dict] | pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_seller: pd.DataFrame,
    dim_customer: pd.DataFrame,
) -> pd.DataFrame:
    """Construye `fact_sales` con grano de 1 fila por línea de orden (`order_id` + `line_number`).

    - Deduplica eventos `order_created` por `order_id`.
    - Aplica el último estado conocido de `order_status_changed` si existe.
    - Conserva `event_id` del `order_created` para cumplir trazabilidad RNF-06 (`Gold -> Bronze`).
    - Calcula `gross_value = quantity * unit_price` y `net_value = quantity * unit_price - discount`.
    """
    cols = [
        "sale_line_id", "event_id", "order_id", "line_number",
        "product_key", "product_id", "category_id",
        "seller_key", "seller_id",
        "customer_key", "user_id",
        "location_key", "city", "department",
        "datetime_key", "date", "event_ts", "event_date",
        "payment_method", "order_status",
        "quantity", "unit_price", "discount", "gross_value", "net_value",
    ]
    df = pd.DataFrame(orders_events) if not isinstance(orders_events, pd.DataFrame) else orders_events.copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    seller_key_by_id = dict(zip(dim_seller["seller_id"], dim_seller["seller_key"])) if not dim_seller.empty else {}
    cust_key_by_id = dict(zip(dim_customer["customer_id"], dim_customer["customer_key"])) if not dim_customer.empty else {}

    @lru_cache(maxsize=None)
    def product_version(product_id: str, event_date: str) -> dict | None:
        return resolve_product_version(dim_product, product_id, event_date)

    # Estado más reciente por order_id si hay cambios de estado
    latest_status: dict[str, tuple[str, str]] = {}
    for r in df.to_dict("records"):
        oid = r.get("order_id")
        st = r.get("status")
        ts = _norm_ts_str(r.get("event_ts"))
        if oid and st:
            prev = latest_status.get(oid)
            if prev is None or ts >= prev[0]:
                latest_status[oid] = (ts, str(st))

    created = [r for r in df.to_dict("records") if r.get("event_type") == "order_created"]
    seen_orders: set[str] = set()
    rows = []

    for ev in created:
        oid = str(ev["order_id"])
        if oid in seen_orders:
            continue
        seen_orders.add(oid)

        ev_id = str(ev.get("event_id") or oid)
        ts_str = _norm_ts_str(ev.get("event_ts"))
        order_date = ts_str[:10]
        dt_key = ts_str[:16]
        ev_date = pd.to_datetime(order_date).date()
        city = str(ev.get("city") or "Desconocida")
        dept = str(ev.get("department") or "Desconocido")
        loc_key = surrogate_key("LOC", city, dept)
        uid = ev.get("user_id")
        uid_str = str(uid) if uid is not None and not (isinstance(uid, float) and pd.isna(uid)) else None
        cust_key = cust_key_by_id.get(uid_str, surrogate_key("CUST", uid_str or "ANON"))

        pay = ev.get("payment")
        if isinstance(pay, dict):
            pay_method = str(pay.get("method", "card"))
        else:
            pay_method = str(ev.get("payment_method") or "card")

        status = latest_status.get(oid, (ts_str, str(ev.get("status") or "paid")))[1]
        lines = _parse_lines(ev.get("lines"))

        for idx, line in enumerate(lines, start=1):
            pid = str(line["product_id"])
            p_ver = product_version(pid, order_date)
            if p_ver:
                p_key = str(p_ver["product_key"])
                cat_id = int(p_ver["category_id"])
                s_id = str(p_ver["seller_id"])
            else:
                p_key = surrogate_key("PROD", pid, order_date)
                cat_id = int(line.get("category_id") or 1)
                s_id = str(line.get("seller_id") or "S0001")

            s_key = seller_key_by_id.get(s_id, surrogate_key("SELLER", s_id))
            qty = int(line["quantity"])
            u_price = float(line["unit_price"])
            disc = float(line.get("discount") or 0.0)
            gross = qty * u_price
            net = gross - disc

            rows.append({
                "sale_line_id": f"{oid}-{idx}",
                "event_id": ev_id,
                "order_id": oid,
                "line_number": idx,
                "product_key": p_key,
                "product_id": pid,
                "category_id": cat_id,
                "seller_key": s_key,
                "seller_id": s_id,
                "customer_key": cust_key,
                "user_id": uid_str,
                "location_key": loc_key,
                "city": city,
                "department": dept,
                "datetime_key": dt_key,
                "date": order_date,
                "event_ts": pd.to_datetime(ts_str, utc=True).to_pydatetime(),
                "event_date": ev_date,
                "payment_method": pay_method,
                "order_status": status,
                "quantity": qty,
                "unit_price": u_price,
                "discount": disc,
                "gross_value": gross,
                "net_value": net,
            })

    return pd.DataFrame(rows, columns=cols)


def build_fact_funnel_df(
    click_events: list[dict] | pd.DataFrame,
    dim_product: pd.DataFrame,
    orders_events: list[dict] | pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Construye `fact_funnel_event` para el embudo de conversión (RF-10).

    - Etapas del embudo: `page_view`, `add_to_cart`, `purchase`.
    - Si un evento `purchase` viene con `product_id` directo (ej. prueba unitaria de `funnel_oracle`),
      usa ese `product_id`.
    - Si `purchase` viene con `product_id = None` y `order_id` (como emite `EventFactory._step`),
      resuelve los productos comprados desde las líneas de `order_created` de ese `order_id` (o desde
      los productos agregados al carrito en esa misma `session_id`).
    - Conserva `event_id` original para cumplir RNF-06.
    """
    cols = [
        "funnel_event_id", "event_id", "session_id", "user_id", "stage",
        "product_key", "product_id", "category_id",
        "device_key", "device",
        "location_key", "city", "department",
        "datetime_key", "hour", "event_ts", "event_date", "order_id",
    ]
    df = pd.DataFrame(click_events) if not isinstance(click_events, pd.DataFrame) else click_events.copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    @lru_cache(maxsize=None)
    def product_version(product_id: str, event_date: str) -> dict | None:
        return resolve_product_version(dim_product, product_id, event_date)

    # Mapa order_id -> lista de product_id desde orders_events
    order_products: dict[str, list[str]] = {}
    if orders_events is not None:
        odf = pd.DataFrame(orders_events) if not isinstance(orders_events, pd.DataFrame) else orders_events
        if not odf.empty:
            for r in odf.to_dict("records"):
                if r.get("event_type") == "order_created" and r.get("order_id"):
                    pids = [str(l["product_id"]) for l in _parse_lines(r.get("lines")) if l.get("product_id")]
                    if pids:
                        order_products[str(r["order_id"])] = pids

    # Respaldo: productos metidos al carrito por session_id
    session_cart_products: dict[str, list[str]] = {}
    for r in df.to_dict("records"):
        if r.get("event_type") == "add_to_cart" and r.get("session_id") and r.get("product_id"):
            sid = str(r["session_id"])
            pid = str(r["product_id"])
            session_cart_products.setdefault(sid, [])
            if pid not in session_cart_products[sid]:
                session_cart_products[sid].append(pid)

    rows = []
    seen_keys: set[str] = set()

    for idx, ev in enumerate(df.to_dict("records")):
        etype = ev.get("event_type")
        if etype not in FUNNEL_STAGES:
            continue

        sid = str(ev.get("session_id") or "")
        if not sid:
            continue

        raw_pid = ev.get("product_id")
        oid = ev.get("order_id")
        oid_str = str(oid) if oid is not None and not (isinstance(oid, float) and pd.isna(oid)) else None

        if raw_pid is not None and not (isinstance(raw_pid, float) and pd.isna(raw_pid)) and str(raw_pid).strip():
            pids = [str(raw_pid)]
        elif etype == "purchase":
            if oid_str and oid_str in order_products:
                pids = order_products[oid_str]
            elif sid in session_cart_products:
                pids = session_cart_products[sid]
            else:
                continue
        else:
            continue

        ev_id = str(ev.get("event_id") or f"ev-{idx}")
        ts_str = _norm_ts_str(ev.get("event_ts"))
        ev_day = ts_str[:10]
        dt_key = ts_str[:16]
        hour_bucket = ts_str[:13]
        ev_date = pd.to_datetime(ev_day).date()
        device = str(ev.get("device") or "app")
        dev_key = surrogate_key("DEV", device)
        city = str(ev.get("city") or "Desconocida")
        dept = str(ev.get("department") or "Desconocido")
        loc_key = surrogate_key("LOC", city, dept)
        uid = ev.get("user_id")
        uid_str = str(uid) if uid is not None and not (isinstance(uid, float) and pd.isna(uid)) else None

        for pid in pids:
            f_id = f"{ev_id}:{pid}"
            if f_id in seen_keys:
                continue
            seen_keys.add(f_id)

            p_ver = product_version(pid, ev_day)
            if p_ver:
                p_key = str(p_ver["product_key"])
                cat_id = int(p_ver["category_id"])
            else:
                p_key = surrogate_key("PROD", pid, ev_day)
                cat_id = int(ev.get("category_id") or 1)

            rows.append({
                "funnel_event_id": f_id,
                "event_id": ev_id,
                "session_id": sid,
                "user_id": uid_str,
                "stage": str(etype),
                "product_key": p_key,
                "product_id": pid,
                "category_id": cat_id,
                "device_key": dev_key,
                "device": device,
                "location_key": loc_key,
                "city": city,
                "department": dept,
                "datetime_key": dt_key,
                "hour": hour_bucket,
                "event_ts": pd.to_datetime(ts_str, utc=True).to_pydatetime(),
                "event_date": ev_date,
                "order_id": oid_str,
            })

    return pd.DataFrame(rows, columns=cols)


def build_agg_product_affinity_df(fact_sales: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
    """Construye `agg_product_affinity` (RF-13): pares de productos comprados juntos en la misma orden."""
    cols = ["product_id", "related_product_id", "co_orders", "rank"]
    if fact_sales is None or fact_sales.empty:
        return pd.DataFrame(columns=cols)

    pairs: dict[tuple[str, str], set[str]] = {}
    for oid, grp in fact_sales.groupby("order_id"):
        prods = sorted({str(p) for p in grp["product_id"].dropna().unique()})
        if len(prods) < 2:
            continue
        for i, pa in enumerate(prods):
            for pb in prods[i + 1:]:
                pairs.setdefault((pa, pb), set()).add(str(oid))
                pairs.setdefault((pb, pa), set()).add(str(oid))

    if not pairs:
        return pd.DataFrame(columns=cols)

    rows = [
        {"product_id": pa, "related_product_id": pb, "co_orders": len(orders_set)}
        for (pa, pb), orders_set in pairs.items()
    ]
    df = pd.DataFrame(rows)
    df = df.sort_values(["product_id", "co_orders", "related_product_id"], ascending=[True, False, True])
    df["rank"] = df.groupby("product_id").cumcount() + 1
    df = df[df["rank"] <= top_k].reset_index(drop=True)
    return df[cols]


def build_gold_star_from_data(
    clicks: list[dict] | pd.DataFrame,
    orders: list[dict] | pd.DataFrame,
    masters_snapshots: list[tuple[str, dict]],
    top_affinity_k: int = 10,
) -> dict[str, pd.DataFrame]:
    """Construye todas las tablas del Esquema en Estrella de Gold en memoria (Pandas)."""
    dim_product = None
    last_sellers = pd.DataFrame()
    last_customers = pd.DataFrame()

    for snap_date, mdata in masters_snapshots:
        prods = mdata["products"].values() if isinstance(mdata["products"], dict) else mdata["products"]
        dim_product = apply_scd2_products_df(dim_product, list(prods) if not isinstance(prods, pd.DataFrame) else prods, snap_date)
        if "sellers" in mdata:
            s = mdata["sellers"].values() if isinstance(mdata["sellers"], dict) else mdata["sellers"]
            last_sellers = pd.DataFrame(list(s)) if not isinstance(s, pd.DataFrame) else s
        if "customers" in mdata:
            c = mdata["customers"].values() if isinstance(mdata["customers"], dict) else mdata["customers"]
            last_customers = pd.DataFrame(list(c)) if not isinstance(c, pd.DataFrame) else c

    if dim_product is None:
        dim_product = apply_scd2_products_df(None, [], "2026-01-01")

    dim_seller = build_dim_seller_df(last_sellers)
    dim_customer = build_dim_customer_df(last_customers)

    fact_sales = build_fact_sales_df(orders, dim_product, dim_seller, dim_customer)
    fact_funnel = build_fact_funnel_df(clicks, dim_product, orders)
    agg_affinity = build_agg_product_affinity_df(fact_sales, top_k=top_affinity_k)

    # Recolectar ubicaciones y timestamps de maestros y hechos
    locs: list[tuple[str, str]] = []
    for df_loc in (dim_seller, dim_customer, fact_sales, fact_funnel):
        if not df_loc.empty and "city" in df_loc.columns and "department" in df_loc.columns:
            locs.extend(zip(df_loc["city"], df_loc["department"]))
    dim_location = build_dim_location_df(locs)

    devs = list(fact_funnel["device"].dropna().unique()) if not fact_funnel.empty else []
    dim_device = build_dim_device_df(devs)

    ts_list: list[str] = []
    if not fact_sales.empty:
        ts_list.extend(fact_sales["datetime_key"].dropna().astype(str).tolist())
    if not fact_funnel.empty:
        ts_list.extend(fact_funnel["datetime_key"].dropna().astype(str).tolist())
    dim_datetime = build_dim_datetime_df(ts_list)

    return {
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "dim_customer": dim_customer,
        "dim_location": dim_location,
        "dim_device": dim_device,
        "dim_datetime": dim_datetime,
        "fact_funnel_event": fact_funnel,
        "fact_sales": fact_sales,
        "agg_product_affinity": agg_affinity,
    }
