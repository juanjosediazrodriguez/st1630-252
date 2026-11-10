"""Fase B · integración de Q2..Q5: escenarios de P1 -> archivos por tramos -> job completo (--run_once) -> verificadores de P1.

Con availableNow el watermark solo avanza entre micro-batches: por eso los eventos se escriben en varios archivos
por tramos de tiempo y cada consulta lee con maxFilesPerTrigger = 1.
"""
import pytest
from datetime import datetime, timedelta, timezone

import pandas as pd

from helpers import iso_df, make_cfg, read_delta, read_jsonl, write_slices
from tiendacol_gen import scenarios as sc
from tiendacol_gen import verify as v
from tiendacol_stream.job import run_job


def parse_iso(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


# ------------------------------------------------------------------ Q2 · RF-03
def test_rf03_stock_actual_coincide_con_el_maestro_mas_los_cambios(spark, lake, masters_p1):
    s = sc.rf03(masters_p1)
    cfg = make_cfg(lake, queries=("ingesta", "stock"))
    write_slices(s.items, lake / "input", n_slices=5)
    run_job(cfg, spark)
    stock = read_delta(spark, cfg.stock_current)
    by_product = stock.groupby("product_id", as_index=False)["stock"].sum()
    res = v.verify_rf03(s.expected, by_product)
    print(res)
    assert res.ok, res
    # el stock por (producto, bodega) también coincide con el cálculo manual del escenario
    por_bodega = {f"{r.product_id}|{r.warehouse_id}": r.stock for r in stock.itertuples()}
    for k, esperado in s.expected["stock_by_product_warehouse"].items():
        assert por_bodega[k] == esperado
    # todo el inventario del maestro está sembrado (también los productos sin movimientos)
    assert len(stock) == len(masters_p1.inventory)
    assert (stock["cumulative_delta"].abs().sum() > 0) and (stock["stock"] == stock["base_stock"] + stock["cumulative_delta"]).all()


def test_rf03_reintentar_el_lote_de_stock_no_cambia_el_resultado(spark, lake, masters_p1):
    from tiendacol_stream import stock as st
    s = sc.rf03(masters_p1)
    cfg = make_cfg(lake, queries=("ingesta", "stock"))
    write_slices(s.items, lake / "input", n_slices=2)
    run_job(cfg, spark)
    antes = read_delta(spark, cfg.stock_current).sort_values(["product_id", "warehouse_id"]).reset_index(drop=True)
    # mismo acumulado dos veces (lo que haría Spark al reintentar el foreachBatch)
    agg = st.aggregate_stock(spark.read.format("delta").load(cfg.silver))
    st.process_batch(spark, agg, 99, cfg)
    st.process_batch(spark, agg, 99, cfg)
    despues = read_delta(spark, cfg.stock_current).sort_values(["product_id", "warehouse_id"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(antes.drop(columns="updated_at"), despues.drop(columns="updated_at"))


# ------------------------------------------------------------------ Q5 · RF-09
def test_rf09_ventas_por_minuto(spark, lake, masters_p1):
    s = sc.rf09(masters_p1)                                  # 12 órdenes (cat. 1, COP 1.200.000) + 8 (cat. 3, COP 400.000)
    cfg = make_cfg(lake, queries=("ingesta", "ventas_minuto"))
    write_slices(s.items, lake / "input", n_slices=4)
    run_job(cfg, spark)
    rows = iso_df(spark, cfg.agg_sales_minute, ["minute_start", "minute_end", "updated_at"])
    mine = rows[rows["minute_start"].str.startswith(s.expected["minute_start"][:16])].rename(columns={"sales_cop": "value"})
    res = v.verify_rf09(s.expected, mine)
    print(res)
    assert res.ok, res
    tot = mine[mine["category_id"].isna()].iloc[0]
    assert (tot["orders"], tot["value"], tot["category"]) == (20, 1_600_000.0, "Total")
    por_cat = {int(r.category_id): (r.orders, r.value, r.category) for r in mine.dropna(subset=["category_id"]).itertuples()}
    assert por_cat == {1: (12, 1_200_000.0, "Tecnología"), 3: (8, 400_000.0, "Moda")}
    assert len(mine) == 3                                    # una fila total + una por categoría, sin duplicados por lote


# ------------------------------------------------------------------ Q3 · RF-07
def test_rf07_alerta_de_agotamiento_en_la_venta_65(spark, lake, masters_p1):
    s = sc.rf07(masters_p1)
    e = s.expected
    assert e["trigger_index"] == 64                           # la venta #65 (índice 64)
    cfg = make_cfg(lake, queries=("ingesta", "alertas"))
    # Primer tramo: ventas 0..59 (2 eventos por venta: orden + cambio de stock); después un tramo por venta
    # para que cada micro-batch de Q3 vea una venta más y la alerta pueda salir exactamente en la #65.
    bounds = [120] + [120 + 2 * k for k in range(1, 16)]
    write_slices(s.items, lake / "input", bounds=bounds, single_stream="orders")
    run_job(cfg, spark)
    alerts = read_jsonl(lake / "decisions" / "alerts.jsonl")
    print("ALERTAS", [(a["alert_id"], a["last_sale_ts"], a["stock"], a["units_15m"]) for a in alerts])
    assert len(alerts) == 1, "debe haber una sola alerta: el enfriamiento evita repetirla en las ventas 66..75"
    a = alerts[0]
    assert a["product_id"] == "TEST-RF07" and a["decision_type"] == "stock_depletion_alert" and a["schema_version"] == "1.0"
    assert a["last_sale_ts"] == e["trigger_ts"]               # salió por la venta #65 y no por la #64
    assert a["stock"] == e["stock_at_trigger"] == 255 and a["units_15m"] == 65
    assert abs(a["velocity_per_min"] - 65 / 15) < 1e-3 and abs(a["coverage_min"] - 58.8) < 0.05
    assert a["alert_id"] == f"TEST-RF07|{a['window_end']}"
    # la venta #64 (índice 63) quedó en el límite exacto de 60 min y no alertó
    previa = parse_iso(e["trigger_ts"]) - timedelta(seconds=12)
    assert parse_iso(a["last_sale_ts"]) > previa
    # alert_log tiene la alerta una sola vez
    log = read_delta(spark, cfg.alert_log)
    assert list(log["alert_id"]) == [a["alert_id"]]
    # latencia hacia Operaciones: la alerta llega después de la venta que la dispara (no antes)
    assert parse_iso(a["received_at"]) >= parse_iso(a["detected_at"]) - timedelta(seconds=1)


# ------------------------------------------------------------------ Q4 · RF-08
def test_rf08_carrito_abandonado_con_latido_de_trafico(spark, lake, masters_p1):
    s = sc.rf08(masters_p1)
    e = s.expected
    cfg = make_cfg(lake, queries=("ingesta", "carritos"))
    write_slices(s.items, lake / "input", n_slices=14, single_stream="clickstream")
    run_job(cfg, spark)
    msgs = read_jsonl(lake / "decisions" / "carts.jsonl")
    print("CARRITOS", [(m["user_id"], m["abandoned_at"], m["cart_value"]) for m in msgs])
    users = [m["user_id"] for m in msgs]
    assert users == ["U-TEST-A"], users                       # B compró, C no llega al umbral, NOISE solo manda latidos
    m = msgs[0]
    a_info = e["present"]["U-TEST-A"]
    assert m["cart_value"] == 600000.0 and m["currency"] == "COP" and m["decision_type"] == "abandoned_cart"
    assert m["last_event_ts"] == a_info["last_event_ts"]
    # abandoned_at = último evento + 30 min = el primer instante permitido de la ventana [30, 35] min del E1
    assert m["abandoned_at"] == a_info["earliest"]
    assert parse_iso(m["abandoned_at"]) == parse_iso(m["last_event_ts"]) + timedelta(minutes=30)
    assert m["message_id"] == f"U-TEST-A|{m['session_start']}" and m["events_count"] == 1
    assert m["product_ids"] == ["TEST-RF09-A"]
    # el verificador de P1: la aparición debe caer en [earliest, deadline]; aquí se evalúa con el instante abandoned_at
    first_seen = {u: (parse_iso(m["abandoned_at"]) if u == "U-TEST-A" else None) for u in list(e["present"]) + list(e["absent"])}
    assert v.verify_rf08(e, first_seen).ok
