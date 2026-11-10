"""Pruebas automatizadas del módulo P3 (Esquema Estrella Gold, SCD Tipo 2, RF-10, RF-11, RF-13 y RNF-06).

Importa el generador y los verificadores oficiales de P1 (`tiendacol_gen`) para comprobar
matemáticamente que:
1. `dim_product` aplica SCD Tipo 2 cuando un producto cambia de precio o categoría entre días,
   y es 100% idempotente si se ejecuta dos veces sobre el mismo snapshot.
2. El join estrella de `fact_funnel_event` coincide al 100% con `funnel_oracle` (RF-10).
3. El join estrella de `fact_sales` coincide al 100% con `sales_oracle` (RF-11: ventas, órdenes
   únicas y ticket promedio = ventas / órdenes).
4. `agg_product_affinity` pasa el verificador oficial `verify_rf13` sobre el escenario `rf13` (RF-13).
5. El 100% de las filas de `fact_sales` y `fact_funnel_event` pasan `verify_rnf06` (trazabilidad Gold -> Bronze).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "p1-generador"))

from tiendacol_gen import scenarios as sc  # noqa: E402
from tiendacol_gen import verify as v  # noqa: E402
from tiendacol_gen.events import EventFactory  # noqa: E402
from tiendacol_gen.masters import build_masters, write_masters  # noqa: E402

from tiendacol_gold.config import parse_config  # noqa: E402
from tiendacol_gold.dimensions import (  # noqa: E402
    apply_scd2_products_df,
    load_local_masters_snapshots,
    resolve_product_version,
)
from tiendacol_gold.facts import build_gold_star_from_data  # noqa: E402
from tiendacol_gold.queries import (  # noqa: E402
    query_rf10_funnel,
    query_rf11_sales,
    query_scd2_history,
)


@pytest.fixture(scope="module")
def masters_day0():
    return build_masters(seed=42, day_index=0, n_products=200, n_customers=400, n_sellers=20)


@pytest.fixture(scope="module")
def masters_day1():
    return build_masters(seed=42, day_index=1, n_products=200, n_customers=400, n_sellers=20)


def test_config_parser_ignores_glue_unknown_args():
    cfg = parse_config([
        "--lake_root", "s3://mi-bucket-lake/",
        "--process_date", "2026-11-09",
        "--JOB_NAME", "tiendacol-gold-batch",
        "--JOB_RUN_ID", "jr_12345",
    ])
    assert cfg.lake_root == "s3://mi-bucket-lake"
    assert cfg.masters_root == "s3://mi-bucket-lake/landing/masters"
    assert cfg.process_date == "2026-11-09"
    assert cfg.fact_sales == "s3://mi-bucket-lake/gold/fact_sales"


def test_scd2_product_history_and_idempotency(masters_day0, masters_day1):
    # Día 1: carga inicial
    d0 = apply_scd2_products_df(None, list(masters_day0.products.values()), "2026-01-01")
    assert len(d0) == len(masters_day0.products)
    assert d0["is_current"].all()
    assert d0["valid_to"].isna().all()

    # Re-ejecutar el mismo día no debe duplicar ni cerrar filas (idempotencia)
    d0_retry = apply_scd2_products_df(d0, list(masters_day0.products.values()), "2026-01-01")
    assert len(d0_retry) == len(d0)
    assert d0_retry["is_current"].all()

    # Día 2: deriva diaria (2% cambia precio, 0.5% cambia categoría, 1% productos nuevos)
    d1 = apply_scd2_products_df(d0_retry, list(masters_day1.products.values()), "2026-01-02")
    closed = d1[d1["is_current"] == False]  # noqa: E712
    current = d1[d1["is_current"] == True]  # noqa: E712

    assert len(closed) > 0, "Deben existir versiones cerradas por cambio de precio/categoría"
    assert (closed["valid_to"] == "2026-01-02").all()
    assert len(current) == len(masters_day1.products)
    assert current["product_id"].nunique() == len(current), "Solo puede haber 1 versión vigente por product_id"

    # Verificar Point-in-Time lookup para un producto que cambió
    changed_pid = closed.iloc[0]["product_id"]
    v_old = resolve_product_version(d1, changed_pid, "2026-01-01")
    v_new = resolve_product_version(d1, changed_pid, "2026-01-02")
    assert v_old["product_key"] != v_new["product_key"]
    assert not v_old["is_current"] and v_new["is_current"]


def test_rf10_funnel_matches_p1_oracle_exact(masters_day0):
    factory = EventFactory(masters_day0, seed=7, dup_rate=0.0, late_rate=0.0, malformed_rate=0.0)
    records = factory.next_batch(600, now=datetime(2026, 11, 8, 14, 20, tzinfo=timezone.utc))
    events = [r.event for r in records if r.event]

    clicks = [e for e in events if e["event_type"] in ("page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start", "purchase")]
    orders = [e for e in events if e["event_type"] in ("order_created", "order_status_changed")]

    # Para comparar contra funnel_oracle de P1, enriquecemos purchase con product_id si viene por order_id
    order_first_pid = {
        o["order_id"]: o["lines"][0]["product_id"]
        for o in orders
        if o["event_type"] == "order_created" and o.get("lines")
    }
    clicks_for_oracle = []
    for c in clicks:
        c_copy = dict(c)
        if c_copy["event_type"] == "purchase" and not c_copy.get("product_id") and c_copy.get("order_id") in order_first_pid:
            c_copy["product_id"] = order_first_pid[c_copy["order_id"]]
        clicks_for_oracle.append(c_copy)

    star = build_gold_star_from_data(
        clicks_for_oracle,
        orders,
        [("2026-01-01", {"products": masters_day0.products, "sellers": masters_day0.sellers, "customers": masters_day0.customers})],
    )

    got_funnel = query_rf10_funnel(star)
    exp_funnel = v.funnel_oracle(clicks_for_oracle, masters_day0)

    res = v.compare_frames(
        "RF-10 Embudo Gold vs Oráculo P1",
        exp_funnel,
        got_funnel,
        keys=["category_id", "device", "city", "hour"],
        cols=["page_view", "add_to_cart", "purchase", "view_to_cart", "cart_to_purchase", "view_to_purchase"],
    )
    assert res.ok, f"Discrepancia en RF-10: {res}"


def test_rf11_sales_and_avg_ticket_matches_p1_oracle_exact(masters_day0):
    s_rf02 = sc.rf02(masters_day0)
    orders = [i.record.event for i in s_rf02.items if i.record.event]

    star = build_gold_star_from_data(
        [],
        orders,
        [("2026-01-01", {"products": masters_day0.products, "sellers": masters_day0.sellers, "customers": masters_day0.customers})],
    )

    got_sales = query_rf11_sales(star)
    exp_sales = v.sales_oracle(orders, masters_day0)

    res = v.compare_frames(
        "RF-11 Ventas y Ticket Promedio Gold vs Oráculo P1",
        exp_sales,
        got_sales,
        keys=["date", "category_id", "seller_id", "city", "department"],
        cols=["sales", "orders", "avg_ticket"],
    )
    assert res.ok, f"Discrepancia en RF-11: {res}"

    # Verificar que los cambios de estado (order_status_changed) actualizaron order_status en fact_sales
    statuses = set(star["fact_sales"]["order_status"].unique())
    assert "packed" in statuses


def test_rf13_product_affinity_passes_p1_verifier(masters_day0):
    s_rf13 = sc.rf13(masters_day0)
    orders = [i.record.event for i in s_rf13.items if i.record.event]

    star = build_gold_star_from_data(
        [],
        orders,
        [("2026-01-01", {"products": masters_day0.products, "sellers": masters_day0.sellers, "customers": masters_day0.customers})],
    )

    aff = star["agg_product_affinity"]
    res = v.verify_rf13(s_rf13.expected, aff)
    assert res.ok, f"Falló verificación RF-13: {res}"


def test_rnf06_traceability_gold_to_bronze(masters_day0):
    factory = EventFactory(masters_day0, seed=11, dup_rate=0.05, late_rate=0.0, malformed_rate=0.0)
    records = factory.next_batch(300, now=datetime(2026, 11, 8, 15, 0, tzinfo=timezone.utc))
    events = [r.event for r in records if r.event]

    bronze_df = pd.DataFrame([{"event_id": e["event_id"]} for e in events])
    clicks = [e for e in events if e["event_type"] in ("page_view", "add_to_cart", "purchase")]
    orders = [e for e in events if e["event_type"] in ("order_created", "order_status_changed")]

    star = build_gold_star_from_data(
        clicks,
        orders,
        [("2026-01-01", {"products": masters_day0.products, "sellers": masters_day0.sellers, "customers": masters_day0.customers})],
    )

    gold_events = pd.concat([
        star["fact_sales"][["event_id"]],
        star["fact_funnel_event"][["event_id"]],
    ], ignore_index=True)

    res = v.verify_rnf06(gold_events, bronze_df)
    assert res.ok, f"Falló trazabilidad RNF-06: {res}"


def test_local_masters_snapshots_loader_and_scd2_query(tmp_path, masters_day0, masters_day1):
    m_root = tmp_path / "landing" / "masters"
    write_masters(masters_day0, m_root, "2026-11-08")
    write_masters(masters_day1, m_root, "2026-11-09")

    snaps = load_local_masters_snapshots(m_root)
    assert len(snaps) == 2
    assert [d for d, _ in snaps] == ["2026-11-08", "2026-11-09"]

    star = build_gold_star_from_data([], [], snaps)
    hist = query_scd2_history(star)
    assert not hist.empty
    assert set(hist["is_current"].unique()) == {True, False}
