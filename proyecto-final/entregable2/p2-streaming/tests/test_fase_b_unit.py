"""Fase B · lógica pura de Q2..Q5 sobre DataFrames de lote (sin streaming)."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from tiendacol_stream.alertas import apply_cooldown, current_windows, evaluate_alerts
from tiendacol_stream.carritos import WATERMARK_KEY, abandoned_sessions, to_messages as cart_messages
from tiendacol_stream.config import Config
from tiendacol_stream.stock import aggregate_stock, stock_rows
from tiendacol_stream.ventas_minuto import minute_agg, order_rows, sales_rows

CFG = Config(lake_root="/x", checkpoint_root="/y")
T0 = datetime(2026, 11, 9, 15, 0, 0)


def at(minutes=0.0, seconds=0.0):
    from datetime import timedelta
    return T0 + timedelta(minutes=minutes, seconds=seconds)


# ------------------------------------------------------------------ Q2
def test_aggregate_stock_suma_delta_por_producto_y_bodega(spark):
    df = spark.createDataFrame([
        ("stock_change", "P1", "BOG", -3, at(0)), ("stock_change", "P1", "BOG", 10, at(1)),
        ("stock_change", "P1", "MED", -1, at(2)), ("page_view", "P1", None, None, at(3)),
        ("stock_change", "P2", "BOG", 5, at(4))],
        "event_type string, product_id string, warehouse_id string, delta int, event_ts timestamp")
    got = {(r.product_id, r.warehouse_id): (r.cumulative_delta, r.last_event_ts) for r in aggregate_stock(df).collect()}
    assert got == {("P1", "BOG"): (7, at(1)), ("P1", "MED"): (-1, at(2)), ("P2", "BOG"): (5, at(4))}


def test_stock_rows_suma_la_base_del_maestro_y_acepta_pares_nuevos(spark):
    upd = spark.createDataFrame([("P1", "BOG", 7, at(1)), ("P9", "BOG", -2, at(2))],
                                "product_id string, warehouse_id string, cumulative_delta long, last_event_ts timestamp")
    inv = spark.createDataFrame([("P1", "BOG", 100)], "product_id string, warehouse_id string, base_stock int")
    got = {r.product_id: (r.base_stock, r.cumulative_delta, r.stock, r.masters_dt) for r in stock_rows(upd, inv, "2026-11-09").collect()}
    assert got == {"P1": (100, 7, 107, "2026-11-09"), "P9": (0, -2, -2, "2026-11-09")}


# ------------------------------------------------------------------ Q3
def _cands(spark, rows):
    return spark.createDataFrame(rows, "product_id string, product_name string, category_id int, window_start timestamp, "
                                       "window_end timestamp, units_15m long, last_sale_ts timestamp, stock long")


@pytest.mark.skip(reason="las cubre test_alertas/test_carritos")
def test_regla_de_alerta_en_el_limite_exacto(spark):
    """64 ventas con stock 256 = cobertura exacta de 60 min: NO alerta. 65 ventas con stock 255 = 58,8 min: sí."""
    c = _cands(spark, [("A", "a", 1, at(-14), at(1), 64, at(0), 256), ("B", "b", 1, at(-14), at(1), 65, at(0), 255),
                       ("C", "c", 1, at(-14), at(1), 0, at(0), 10), ("D", "d", 1, at(-14), at(1), 10, at(0), 1000)])
    got = {r.product_id: r for r in evaluate_alerts(c, CFG).collect()}
    assert set(got) == {"B"}
    b = got["B"]
    assert b.units_15m == 65 and b.stock == 255
    assert b.velocity_per_min == pytest.approx(65 / 15) and b.coverage_min == pytest.approx(255 * 15 / 65)
    assert b.alert_id == "B|2026-11-09T15:01:00.000Z"       # determinista: producto + fin de ventana


@pytest.mark.skip(reason="las cubre test_alertas/test_carritos")
def test_current_windows_deja_una_ventana_por_producto(spark):
    # Tres ventanas deslizantes del mismo producto; la vigente termina en el minuto siguiente a la última venta (15:00:30 -> 15:01)
    w = lambda end: (end - __import__("datetime").timedelta(minutes=15), end)  # noqa: E731
    rows = [("A", *w(at(1)), 5, at(0, 30), "a", 1), ("A", *w(at(2)), 5, at(0, 30), "a", 1), ("A", *w(at(0)), 4, at(-1), "a", 1)]
    df = spark.createDataFrame(rows, "product_id string, s timestamp, e timestamp, units_15m long, last_sale_ts timestamp, "
                                     "product_name string, category_id int").select(
        "product_id", F.struct(F.col("s").alias("start"), F.col("e").alias("end")).alias("window"),
        "units_15m", "last_sale_ts", "product_name", "category_id")
    out = current_windows(df, CFG).collect()
    assert len(out) == 1 and out[0].window_end == at(1) and out[0].units_15m == 5


@pytest.mark.skip(reason="las cubre test_alertas/test_carritos")
def test_enfriamiento_por_tiempo_de_evento(spark):
    al = spark.createDataFrame([("A", at(0)), ("A", at(0)), ("B", at(0))], "product_id string, last_sale_ts timestamp") \
        .withColumn("alert_id", F.concat_ws("|", "product_id", F.col("last_sale_ts").cast("string")))
    log = spark.createDataFrame([("A", at(-30)), ("B", at(-61))], "product_id string, last_sale_ts timestamp")
    kept = {r.product_id for r in apply_cooldown(al, log, CFG).collect()}
    assert kept == {"B"}                         # A tiene una alerta hace 30 min (< 60); B hace 61 min (>= 60)
    assert apply_cooldown(al, log.where("1 = 0"), CFG).count() == 3


# ------------------------------------------------------------------ Q4
SILVER_CLICK = ("event_ts timestamp, event_type string, is_identified boolean, user_id string, product_id string, "
                "quantity int, unit_price double, list_price double")


@pytest.mark.skip(reason="las cubre test_alertas/test_carritos")
def test_carrito_abandonado_reglas(spark):
    rows = [
        # A: 6 x 100.000 = 600.000, sin compra -> abandonado
        (at(0), "add_to_cart", True, "A", "P1", 6, 100000.0, 100000.0),
        # B: 600.000 pero compra -> no
        (at(0), "add_to_cart", True, "B", "P1", 6, 100000.0, 100000.0), (at(5), "purchase", True, "B", None, None, None, None),
        # C: 300.000 -> bajo el umbral
        (at(0), "add_to_cart", True, "C", "P1", 3, 100000.0, 100000.0),
        # D: agrega 7 y quita 2 sin precio (usa list_price 100.000) = 500.000 -> justo en el umbral, sí
        (at(0), "add_to_cart", True, "D", "P1", 7, 100000.0, 100000.0), (at(1), "remove_from_cart", True, "D", "P1", 2, None, 100000.0),
        # E: anónimo con carrito grande -> no cuenta
        (at(0), "add_to_cart", False, None, "P1", 9, 100000.0, 100000.0),
        # F: vistas de página NO extienden la sesión: último evento de carrito en el min 0
        (at(0), "add_to_cart", True, "F", "P1", 6, 100000.0, 100000.0), (at(20), "page_view", True, "F", "P1", None, None, None),
        # reloj de fondo
        (at(90), "page_view", False, None, "P1", None, None, None)]
    df = spark.createDataFrame(rows, SILVER_CLICK)
    out = {r.user_id: r for r in abandoned_sessions(df, CFG).collect()}
    assert set(out) == {"A", "D", "F"}
    assert out["A"].cart_value == 600000.0 and out["A"].abandoned_at == at(30) and out["A"].last_event_ts == at(0)
    assert out["D"].cart_value == 500000.0 and out["D"].abandoned_at == at(31)      # el último evento de carrito fue el min 1
    assert out["F"].abandoned_at == at(30)                                          # la vista del min 20 no extendió la sesión
    assert out["A"].product_ids == ["P1"] and out["A"].events_count == 1
    assert WATERMARK_KEY not in out


@pytest.mark.skip(reason="las cubre test_alertas/test_carritos")
def test_mensaje_de_carrito_abandonado(spark):
    df = spark.createDataFrame([(at(0), "add_to_cart", True, "U-TEST-A", "P1", 6, 100000.0, 100000.0)], SILVER_CLICK)
    (m,) = cart_messages(abandoned_sessions(df, CFG))
    assert m["decision_type"] == "abandoned_cart" and m["currency"] == "COP" and m["schema_version"] == "1.0"
    assert m["message_id"] == "U-TEST-A|2026-11-09T15:00:00.000Z" and m["abandoned_at"] == "2026-11-09T15:30:00.000Z"
    assert m["last_event_ts"] == "2026-11-09T15:00:00.000Z" and m["abandoned_date"] == "2026-11-09"   # 10:30 en Colombia
    assert m["cart_value"] == 600000.0 and m["events_count"] == 1 and m["product_ids"] == ["P1"]
    assert m["detected_at"].endswith("Z")


# ------------------------------------------------------------------ Q5
LINES = ("array<struct<product_id:string,quantity:int,unit_price:double,discount:double,category_id:int,"
         "seller_id:string,line_value:double>>")


def _orders(spark):
    ln = lambda pid, cat, v: (pid, 1, v, 0.0, cat, "S1", v)  # noqa: E731
    rows = [("order_created", at(0, 5), [ln("a", 1, 100.0), ln("b", 1, 50.0), ln("c", 3, 25.0)]),
            ("order_created", at(0, 30), [ln("a", 1, 100.0)]),
            ("order_created", at(1, 10), [ln("x", None, 10.0)]),      # producto fuera del maestro
            ("page_view", at(0, 6), None)]
    return spark.createDataFrame(rows, f"event_type string, event_ts timestamp, lines {LINES}")


def test_filas_por_orden_y_categoria_suman_el_total(spark):
    rows = order_rows(_orders(spark)).collect()
    por_orden_total = sorted(r.value for r in rows if r.category_id is None)
    assert por_orden_total == [10.0, 100.0, 175.0]
    cat = {}
    for r in rows:
        if r.category_id is not None:
            cat[r.category_id] = cat.get(r.category_id, 0) + r.value
    assert cat == {1: 250.0, 3: 25.0, -1: 10.0}                       # -1 = categoría desconocida, no se mezcla con el total
    assert sum(cat.values()) == sum(por_orden_total)


def test_minute_agg_cuenta_ordenes_y_valor_por_minuto(spark):
    got = {(r.minute_start, r.category_id): (r.orders, r.sales_cop) for r in minute_agg(_orders(spark), CFG).collect()}
    assert got[(at(0), None)] == (2, 275.0) and got[(at(0), 1)] == (2, 250.0) and got[(at(0), 3)] == (1, 25.0)
    assert got[(at(1), None)] == (1, 10.0) and got[(at(1), -1)] == (1, 10.0)


def test_sales_rows_nombra_categorias_y_total(spark):
    agg = minute_agg(_orders(spark), CFG)
    cats = spark.createDataFrame([(1, "Tecnología"), (3, "Moda")], "category_id int, category string")
    names = {(r.category_id): r.category for r in sales_rows(agg, cats).collect()}
    assert names == {None: "Total", 1: "Tecnología", 3: "Moda", -1: "Sin categoría"}
