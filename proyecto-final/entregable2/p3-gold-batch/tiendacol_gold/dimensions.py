"""Construcción de las tablas de dimensiones (Esquema Estrella) con SCD Tipo 2 en dim_product.

Soporta tanto ejecución sobre DataFrames de Pandas (para pruebas unitarias instantáneas y
validación contra el oráculo de P1) como sobre PySpark + Delta Lake (en AWS Glue Batch 4.0).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from .tables import surrogate_key

_MANIFEST_RE = re.compile(r"_manifest_dt=(\d{4}-\d{2}-\d{2})\.json$")
DEVICES = ("desktop", "mobile_web", "app")


# =============================================================================
# 1. Lógica de Dimensiones en Pandas / Python puro (núcleo determinista)
# =============================================================================

def apply_scd2_products_df(
    existing_dim: pd.DataFrame | None,
    incoming_products: list[dict] | pd.DataFrame,
    snapshot_date: str,
) -> pd.DataFrame:
    """Aplica Slowly Changing Dimension Tipo 2 (SCD2) sobre dim_product.

    Reglas:
    - Si un `product_id` no existe en `existing_dim`, se inserta con `valid_from = snapshot_date`,
      `valid_to = None` e `is_current = True`.
    - Si ya existe una fila vigente (`is_current == True`) para ese `product_id`:
      * Si cambió `price`, `category_id`, `category`, `seller_id` o `name`, se cierra la fila
        anterior (`is_current = False`, `valid_to = snapshot_date`) y se inserta una nueva versión
        con `product_key = surrogate_key('PROD', product_id, snapshot_date)`, `valid_from = snapshot_date`,
        `valid_to = None`, `is_current = True`.
      * Si los atributos rastreados no cambiaron (o se reejecuta el mismo `snapshot_date`), se
        conserva sin duplicar (idempotente).
    """
    cols = [
        "product_key", "product_id", "name", "category_id", "category",
        "seller_id", "price", "attributes", "valid_from", "valid_to", "is_current",
    ]
    inc_df = pd.DataFrame(incoming_products) if not isinstance(incoming_products, pd.DataFrame) else incoming_products.copy()
    if inc_df.empty:
        return existing_dim.copy() if existing_dim is not None else pd.DataFrame(columns=cols)

    # Normalizar nombres si vienen del maestro de P2 (product_name / list_price)
    if "product_name" in inc_df.columns and "name" not in inc_df.columns:
        inc_df["name"] = inc_df["product_name"]
    if "list_price" in inc_df.columns and "price" not in inc_df.columns:
        inc_df["price"] = inc_df["list_price"]
    if "attributes" not in inc_df.columns:
        inc_df["attributes"] = "{}"

    inc_df = inc_df.drop_duplicates(subset=["product_id"], keep="last")

    if existing_dim is None or existing_dim.empty:
        rows = []
        for r in inc_df.to_dict("records"):
            pid = str(r["product_id"])
            rows.append({
                "product_key": surrogate_key("PROD", pid, snapshot_date),
                "product_id": pid,
                "name": str(r.get("name", pid)),
                "category_id": int(r["category_id"]),
                "category": str(r["category"]),
                "seller_id": str(r["seller_id"]),
                "price": float(r["price"]),
                "attributes": str(r.get("attributes", "{}")),
                "valid_from": snapshot_date,
                "valid_to": None,
                "is_current": True,
            })
        return pd.DataFrame(rows, columns=cols)

    history = existing_dim.to_dict("records")
    current_idx_by_pid: dict[str, int] = {}
    for idx, row in enumerate(history):
        if bool(row.get("is_current")):
            current_idx_by_pid[str(row["product_id"])] = idx

    for r in inc_df.to_dict("records"):
        pid = str(r["product_id"])
        new_name = str(r.get("name", pid))
        new_cat_id = int(r["category_id"])
        new_cat = str(r["category"])
        new_seller = str(r["seller_id"])
        new_price = float(r["price"])
        new_attrs = str(r.get("attributes", "{}"))

        if pid not in current_idx_by_pid:
            history.append({
                "product_key": surrogate_key("PROD", pid, snapshot_date),
                "product_id": pid,
                "name": new_name,
                "category_id": new_cat_id,
                "category": new_cat,
                "seller_id": new_seller,
                "price": new_price,
                "attributes": new_attrs,
                "valid_from": snapshot_date,
                "valid_to": None,
                "is_current": True,
            })
            current_idx_by_pid[pid] = len(history) - 1
            continue

        cur = history[current_idx_by_pid[pid]]
        changed = (
            int(cur["category_id"]) != new_cat_id
            or str(cur["category"]) != new_cat
            or str(cur["seller_id"]) != new_seller
            or abs(float(cur["price"]) - new_price) > 1e-6
            or str(cur["name"]) != new_name
        )
        if not changed:
            continue

        # Si el mismo snapshot_date se reprocesa con valores actualizados, actualiza in-place
        if str(cur["valid_from"]) == snapshot_date:
            cur.update({
                "name": new_name,
                "category_id": new_cat_id,
                "category": new_cat,
                "seller_id": new_seller,
                "price": new_price,
                "attributes": new_attrs,
            })
        else:
            cur["is_current"] = False
            cur["valid_to"] = snapshot_date
            history.append({
                "product_key": surrogate_key("PROD", pid, snapshot_date),
                "product_id": pid,
                "name": new_name,
                "category_id": new_cat_id,
                "category": new_cat,
                "seller_id": new_seller,
                "price": new_price,
                "attributes": new_attrs,
                "valid_from": snapshot_date,
                "valid_to": None,
                "is_current": True,
            })
            current_idx_by_pid[pid] = len(history) - 1

    return pd.DataFrame(history, columns=cols)


def build_dim_seller_df(sellers: list[dict] | pd.DataFrame) -> pd.DataFrame:
    cols = ["seller_key", "seller_id", "seller_name", "city", "department"]
    df = pd.DataFrame(sellers) if not isinstance(sellers, pd.DataFrame) else sellers.copy()
    if df.empty:
        return pd.DataFrame(columns=cols)
    if "name" in df.columns and "seller_name" not in df.columns:
        df["seller_name"] = df["name"]
    df = df.drop_duplicates(subset=["seller_id"], keep="last")
    df["seller_key"] = df["seller_id"].map(lambda s: surrogate_key("SELLER", s))
    return df[cols].reset_index(drop=True)


def build_dim_customer_df(customers: list[dict] | pd.DataFrame) -> pd.DataFrame:
    cols = ["customer_key", "customer_id", "city", "department", "created_at"]
    df = pd.DataFrame(customers) if not isinstance(customers, pd.DataFrame) else customers.copy()
    if df.empty:
        return pd.DataFrame(columns=cols)
    if "created_at" not in df.columns:
        df["created_at"] = "2026-01-01"
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    df["customer_key"] = df["customer_id"].map(lambda c: surrogate_key("CUST", c))
    return df[cols].reset_index(drop=True)


def build_dim_location_df(locations: list[tuple[str, str]]) -> pd.DataFrame:
    cols = ["location_key", "city", "department"]
    unique_locs = sorted({(str(c), str(d)) for c, d in locations if c and d})
    rows = [
        {"location_key": surrogate_key("LOC", city, dept), "city": city, "department": dept}
        for city, dept in unique_locs
    ]
    return pd.DataFrame(rows, columns=cols)


def build_dim_device_df(devices: list[str] | None = None) -> pd.DataFrame:
    devs = sorted(set(devices or DEVICES) | set(DEVICES))
    rows = [{"device_key": surrogate_key("DEV", d), "device_name": d} for d in devs]
    return pd.DataFrame(rows, columns=["device_key", "device_name"])


def build_dim_datetime_df(timestamps: list[str]) -> pd.DataFrame:
    cols = ["datetime_key", "minute_ts", "date", "hour", "year", "month", "day", "hour_of_day", "minute"]
    minutes = sorted({str(ts)[:16] for ts in timestamps if ts and len(str(ts)) >= 16})
    rows = []
    for m_key in minutes:
        # Formato esperado: YYYY-MM-DDTHH:MM (o con espacio)
        norm = m_key.replace(" ", "T")
        dt_part = norm[:10]
        hr_part = norm[:13]
        year = int(norm[0:4])
        month = int(norm[5:7])
        day = int(norm[8:10])
        hour_of_day = int(norm[11:13])
        minute = int(norm[14:16])
        rows.append({
            "datetime_key": norm,
            "minute_ts": pd.to_datetime(f"{norm}:00Z", utc=True),
            "date": dt_part,
            "hour": hr_part,
            "year": year,
            "month": month,
            "day": day,
            "hour_of_day": hour_of_day,
            "minute": minute,
        })
    return pd.DataFrame(rows, columns=cols)


def resolve_product_version(dim_product: pd.DataFrame, product_id: str, event_date: str) -> dict | None:
    """Busca en dim_product la versión vigente de `product_id` en `event_date` (Point-in-Time lookup)."""
    sub = dim_product[dim_product["product_id"] == product_id]
    if sub.empty:
        return None
    for r in sub.to_dict("records"):
        vf = str(r["valid_from"])
        vt = r["valid_to"]
        if vf <= event_date and (vt is None or pd.isna(vt) or event_date < str(vt)):
            return r
    cur = sub[sub["is_current"] == True]  # noqa: E712
    return (cur.iloc[0] if not cur.empty else sub.iloc[-1]).to_dict()


def load_local_masters_snapshots(masters_root: str | Path) -> list[tuple[str, dict]]:
    """Lee todos los `_manifest_dt=*.json` de `masters_root` en orden cronológico para aplicar SCD2."""
    root = Path(masters_root)
    if not root.exists():
        return []
    manifests = []
    for p in sorted(root.glob("_manifest_dt=*.json")):
        m = _MANIFEST_RE.search(p.name)
        if m:
            manifests.append((m.group(1), p))
    out = []
    for day, mpath in sorted(manifests):
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
        files = manifest.get("files", {})
        data = {}
        for entity in ("products", "customers", "sellers", "inventory"):
            rel = files.get(entity)
            if not rel:
                continue
            fpath = root / rel
            if fpath.suffix == ".json":
                rows = json.loads(fpath.read_text(encoding="utf-8"))
                for r in rows:
                    if isinstance(r.get("attributes"), dict):
                        r["attributes"] = json.dumps(r["attributes"], ensure_ascii=False)
                data[entity] = pd.DataFrame(rows)
            else:
                data[entity] = pd.read_csv(fpath)
        out.append((day, data))
    return out
