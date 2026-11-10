"""Consultas analíticas sobre el Esquema en Estrella de Gold (equivalentes en Python/SQL a consultas_athena.sql).

Permiten comprobar con `compare_frames` de `tiendacol_gen.verify` que los joins entre las tablas
de hechos (`fact_funnel_event`, `fact_sales`) y las dimensiones (`dim_product`, `dim_seller`,
`dim_location`, `dim_device`, `dim_datetime`) producen exactamente el mismo resultado que el
oráculo calculado a mano por P1 (`funnel_oracle`, `sales_oracle` y `verify_rf13`).
"""
from __future__ import annotations

import pandas as pd


def query_rf10_funnel(star: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Ejecuta el join estrella de RF-10 (fact_funnel_event + dim_product + dim_device + dim_location + dim_datetime)."""
    f = star["fact_funnel_event"]
    if f.empty:
        return pd.DataFrame(columns=[
            "category_id", "device", "city", "hour",
            "page_view", "add_to_cart", "purchase",
            "view_to_cart", "cart_to_purchase", "view_to_purchase",
        ])

    p = star["dim_product"][["product_key", "category_id", "category"]].drop_duplicates("product_key")
    d = star["dim_device"][["device_key", "device_name"]].drop_duplicates("device_key")
    l = star["dim_location"][["location_key", "city"]].drop_duplicates("location_key")
    t = star["dim_datetime"][["datetime_key", "hour"]].drop_duplicates("datetime_key")

    joined = (
        f[["session_id", "stage", "product_key", "device_key", "location_key", "datetime_key"]]
        .merge(p, on="product_key", how="inner")
        .merge(d, on="device_key", how="inner")
        .merge(l, on="location_key", how="inner")
        .merge(t, on="datetime_key", how="inner")
    )

    g = (
        joined.groupby(["category_id", "device_name", "city", "hour", "stage"])["session_id"]
        .nunique()
        .unstack("stage", fill_value=0)
        .reset_index()
        .rename(columns={"device_name": "device"})
    )
    for stage_col in ("page_view", "add_to_cart", "purchase"):
        if stage_col not in g.columns:
            g[stage_col] = 0

    g["view_to_cart"] = (g["add_to_cart"] / g["page_view"]).where(g["page_view"] > 0)
    g["cart_to_purchase"] = (g["purchase"] / g["add_to_cart"]).where(g["add_to_cart"] > 0)
    g["view_to_purchase"] = (g["purchase"] / g["page_view"]).where(g["page_view"] > 0)
    return g


def query_rf11_sales(star: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Ejecuta el join estrella de RF-11 (fact_sales + dim_product + dim_seller + dim_location + dim_datetime)."""
    f = star["fact_sales"]
    if f.empty:
        return pd.DataFrame(columns=[
            "date", "category_id", "seller_id", "city", "department",
            "sales", "orders", "avg_ticket",
        ])

    p = star["dim_product"][["product_key", "category_id", "category"]].drop_duplicates("product_key")
    s = star["dim_seller"][["seller_key", "seller_id", "seller_name"]].drop_duplicates("seller_key")
    l = star["dim_location"][["location_key", "city", "department"]].drop_duplicates("location_key")
    t = star["dim_datetime"][["datetime_key", "date"]].drop_duplicates("datetime_key")

    joined = (
        f[["order_id", "net_value", "product_key", "seller_key", "location_key", "datetime_key"]]
        .merge(p, on="product_key", how="inner")
        .merge(s, on="seller_key", how="inner")
        .merge(l, on="location_key", how="inner")
        .merge(t, on="datetime_key", how="inner")
    )

    g = (
        joined.groupby(["date", "category_id", "seller_id", "city", "department"])
        .agg(sales=("net_value", "sum"), orders=("order_id", "nunique"))
        .reset_index()
    )
    g["avg_ticket"] = g["sales"] / g["orders"]
    return g


def query_scd2_history(star: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Devuelve los productos de `dim_product` que tienen múltiples versiones por cambios SCD Tipo 2."""
    dp = star["dim_product"]
    if dp.empty:
        return dp
    counts = dp.groupby("product_id")["product_key"].transform("count")
    return dp[counts > 1].sort_values(["product_id", "valid_from"]).reset_index(drop=True)
