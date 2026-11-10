"""Q4 · carritos abandonados (RF-08): lógica pura, mensaje del contrato, escenario `rf08` de P1 y la trampa del watermark."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from helpers import make_cfg, read_jsonl, write_slices
from tiendacol_gen import scenarios as sc
from tiendacol_stream import carritos as C
from tiendacol_stream.deltaio import read_silver_stream
from tiendacol_stream.job import run_job
from tiendacol_stream.sqs import SqsPublisher
from tiendacol_stream.tables import SILVER_SCHEMA

CFG = make_cfg(Path("/x"))
T0 = datetime(2026, 11, 9, 15, 0, 0)
SCHEMA = ("event_ts timestamp, event_type string, is_identified boolean, user_id string, product_id string, "
          "quantity int, unit_price double, list_price double")


def at(minutes=0.0, seconds=0.0):
    return T0 + timedelta(minutes=minutes, seconds=seconds)


def parse_iso(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


# ------------------------------------------------------------------ lógica pura
def test_la_clave_y_el_importe_de_cada_evento(spark):
    df = spark.createDataFrame([
        (at(0), "add_to_cart", True, "U1", "P1", 2, 100.0, 90.0),           # +2 x 100
        (at(1), "add_to_cart", True, "U1", "P1", 1, None, 90.0),            # sin precio: list_price del maestro
        (at(2), "remove_from_cart", True, "U1", "P1", 1, None, 90.0),       # -1 x 90 (list_price)
        (at(3), "checkout_start", True, "U1", None, None, None, None),       # carrito, importe 0
        (at(4), "page_view", True, "U1", "P1", None, None, 90.0),            # no es de carrito: latido
        (at(5), "add_to_cart", False, None, "P1", 3, 100.0, 90.0)], SCHEMA)  # anónimo: latido, importe 0
    got = [(r.user_key, r.cart_delta, r.is_cart_event) for r in C.cart_signals(df).orderBy("event_ts").collect()]
    assert got == [("U1", 200.0, 1), ("U1", 90.0, 1), ("U1", -90.0, 1), ("U1", 0.0, 1),
                   (C.WATERMARK_KEY, 0.0, 0), (C.WATERMARK_KEY, 0.0, 0)]


def test_sesiones_abandonadas_en_lote(spark):
    rows = [
        (at(0), "add_to_cart", True, "A", "P1", 6, 100000.0, 100000.0),                        # 600.000, sin compra
        (at(0), "add_to_cart", True, "B", "P1", 6, 100000.0, 100000.0), (at(5), "purchase", True, "B", None, None, None, None),
        (at(0), "add_to_cart", True, "C", "P1", 4, 100000.0, 100000.0),                        # 400.000 < umbral
        (at(0), "add_to_cart", True, "D", "P1", 7, 100000.0, 100000.0), (at(1), "remove_from_cart", True, "D", "P1", 2, None, 100000.0),
        (at(0), "add_to_cart", True, "E", "P1", 5, 100000.0, 100000.0), (at(31), "add_to_cart", True, "E", "P1", 1, 100000.0, 100000.0),
        (at(0), "add_to_cart", True, "F", "P1", 6, 100000.0, 100000.0), (at(20), "page_view", True, "F", "P1", None, None, None),
        (at(0), "add_to_cart", False, None, "P1", 9, 100000.0, 100000.0)]
    df = spark.createDataFrame([r if len(r) == 8 else (*r[:5], None, None, None) for r in rows], SCHEMA)
    out = {r.user_id: r for r in C.abandoned_sessions(df, CFG).collect()}
    assert out["A"].cart_value == 600000.0 and out["A"].abandoned_at == at(30) and out["A"].last_event_ts == at(0)
    assert "B" not in out and "C" not in out                    # compró / no llega a 500.000
    assert out["D"].cart_value == 500000.0 and out["D"].abandoned_at == at(31)   # último evento de carrito: min 1
    assert "F" in out and out["F"].abandoned_at == at(30)        # la vista de página no extiende la sesión
    assert C.WATERMARK_KEY not in out and None not in out        # los latidos y anónimos se descartan a la salida
    # E: dos sesiones separadas por más de 30 min; la primera (500.000) se publica, la segunda (100.000) no
    assert out["E"].cart_value == 500000.0 and out["E"].session_start == at(0)


def test_mensaje_del_contrato(spark):
    df = spark.createDataFrame([(datetime(2026, 11, 10, 2, 40), "add_to_cart", True, "U-TEST-A", "P1", 6, 100000.0, 1.0)], SCHEMA)
    (m,) = C.to_messages(C.abandoned_sessions(df, CFG))
    assert set(m) == {"schema_version", "decision_type", "message_id", "user_id", "cart_value", "currency", "session_start",
                      "last_event_ts", "abandoned_at", "abandoned_date", "events_count", "product_ids", "detected_at"}
    assert m["message_id"] == "U-TEST-A|2026-11-10T02:40:00.000Z"        # usuario + inicio de sesión: determinista
    assert m["abandoned_at"] == "2026-11-10T03:10:00.000Z"                # último evento + 30 min
    assert m["abandoned_date"] == "2026-11-09"                            # 22:10 del 9 de noviembre en Colombia (UTC-5)
    assert (m["cart_value"], m["currency"], m["events_count"], m["product_ids"]) == (600000.0, "COP", 1, ["P1"])


class FakePublisher(SqsPublisher):
    def __init__(self, cfg, fail=False):
        super().__init__(cfg)
        self.fail, self.messages = fail, []

    def publish(self, kind, messages):
        if self.fail:
            raise RuntimeError("SQS caído")
        self.messages += [(kind, m) for m in messages]
        return len(messages)


def test_el_lote_publica_en_la_cola_de_carritos_y_si_sqs_falla_el_lote_falla(spark):
    df = spark.createDataFrame([(at(0), "add_to_cart", True, "A", "P1", 6, 100000.0, 1.0)], SCHEMA)
    ab = C.abandoned_sessions(df, CFG)
    pub = FakePublisher(CFG)
    assert C.process_batch(spark, ab, 1, CFG, pub) == 1 and pub.messages[0][0] == "carts"
    with pytest.raises(RuntimeError):                                     # Spark reintenta el lote (at-least-once)
        C.process_batch(spark, ab, 1, CFG, FakePublisher(CFG, fail=True))


# ------------------------------------------------------------------ escenario rf08 de P1
@pytest.fixture(scope="module")
def rf08_silver(spark, tmp_path_factory, masters_p1):
    """Silver de rf08 construido en 14 tramos (un archivo por micro-batch): el watermark avanza entre lotes."""
    from tiendacol_gen.masters import write_masters
    root = tmp_path_factory.mktemp("rf08")
    write_masters(masters_p1, root / "lake" / "landing" / "masters", "2026-11-09")
    s = sc.rf08(masters_p1)
    write_slices(s.items, root / "input", n_slices=14, single_stream="clickstream")
    run_job(make_cfg(root, queries=("ingesta",)), spark)
    return s, root


def _stream(spark, cfg, build, tag):
    """Corre una consulta de prueba sobre Silver (un archivo por lote); devuelve las filas emitidas y el mayor watermark."""
    rows = []
    q = (build(read_silver_stream(spark, cfg), cfg).writeStream.queryName(tag).outputMode("append")
         .foreachBatch(lambda df, _id: rows.extend(df.collect()))
         .option("checkpointLocation", cfg.checkpoint(tag)).trigger(availableNow=True).start())
    q.awaitTermination()
    wms = [p["eventTime"]["watermark"] for p in q.recentProgress if "watermark" in p.get("eventTime", {})]
    return rows, max(wms) if wms else None


def test_rf08_a_aparece_una_vez_b_y_c_no(rf08_silver, spark):
    s, root = rf08_silver
    e = s.expected
    run_job(make_cfg(root, queries=("carritos",)), spark)
    msgs = read_jsonl(root / "decisions" / "carts.jsonl")
    assert [m["user_id"] for m in msgs] == ["U-TEST-A"], msgs           # B compró, C no llega al umbral, NOISE solo late
    m, a = msgs[0], e["present"]["U-TEST-A"]
    assert m["cart_value"] == 600000.0 and m["decision_type"] == "abandoned_cart" and m["currency"] == "COP"
    assert m["last_event_ts"] == a["last_event_ts"]
    assert m["abandoned_at"] == a["earliest"]                            # último evento + 30 min
    assert parse_iso(m["abandoned_at"]) == parse_iso(m["last_event_ts"]) + timedelta(minutes=30)
    assert m["message_id"] == f"U-TEST-A|{m['session_start']}" and m["events_count"] == 1
    assert m["product_ids"] == ["TEST-RF09-A"]


def test_rf08_reprocesar_con_otro_checkpoint_publica_el_mismo_message_id(rf08_silver, spark):
    """Idempotencia: el id es determinista, así que un repetido (at-least-once) lo descarta el upsert de la Lambda."""
    s, root = rf08_silver
    antes = read_jsonl(root / "decisions" / "carts.jsonl")
    run_job(make_cfg(root, queries=("carritos",), checkpoint_root=str(root / "ck2")), spark)
    despues = read_jsonl(root / "decisions" / "carts.jsonl")
    assert [m["message_id"] for m in despues] == [m["message_id"] for m in antes] * 2


# ------------------------------------------------------------------ la trampa del filtro antes del watermark
def _sessions(sig, cfg):
    return (sig.groupBy("user_key", F.session_window("event_ts", f"{cfg.cart_gap_min} minutes"))
               .agg(F.sum("cart_delta").alias("cart_value"), F.max("is_purchase").alias("purchases"))
               .where((F.col("cart_value") >= cfg.cart_min_value) & (F.col("purchases") == 0)))


def _filtrar_antes(silver, cfg):
    cart = silver.where(F.col("event_type").isin(*C.CART_TYPES) & F.col("is_identified"))
    return _sessions(C.cart_signals(cart).withWatermark("event_ts", cfg.cart_watermark), cfg)


def _filtrar_despues_del_watermark(silver, cfg):
    sig = C.cart_signals(silver).withWatermark("event_ts", cfg.cart_watermark)
    return _sessions(sig.where(F.col("user_key") != C.WATERMARK_KEY), cfg)


def test_trampa_sin_la_clave_watermark_la_sesion_de_a_no_se_emite(rf08_silver, spark):
    """El reloj de eventos solo avanza con los datos que llegan al operador de watermark. Si se filtran los eventos que no
    son de carrito, el latido de U-TEST-NOISE (page_view cada 10 s) ya no mueve el reloj: el watermark se queda en el
    último evento de carrito (~min 20, la compra de B), la sesión de A (cierra en el min 30) nunca se cierra y no se emite.
    Con la clave `__watermark__` (consulta real) el latido sí lo mueve y A sale."""
    s, root = rf08_silver
    cfg = make_cfg(root)
    bueno, wm_bueno = _stream(spark, cfg, C.abandoned_sessions, "q4_ok")
    antes, wm_antes = _stream(spark, cfg, _filtrar_antes, "q4_antes")
    despues, wm_despues = _stream(spark, cfg, _filtrar_despues_del_watermark, "q4_despues")
    print("WATERMARKS", wm_bueno, wm_antes, wm_despues)
    print("EMITIDAS", [r.user_id for r in bueno], [r.user_key for r in antes], [r.user_key for r in despues])
    fin_a = parse_iso(s.expected["present"]["U-TEST-A"]["earliest"])           # último evento de A + 30 min
    assert [r.user_id for r in bueno] == ["U-TEST-A"]
    assert antes == []                                                        # filtro antes del watermark: A no sale
    # Evidencia: Spark 3.3 empuja por debajo de withWatermark el filtro que no usa la columna de tiempo. Escribirlo
    # DESPUÉS del watermark da el mismo watermark y el mismo resultado que escribirlo antes.
    assert despues == [] and wm_antes == wm_despues
    # el reloj de la consulta real llega al fin de la sesión de A (por eso se emite); el de las trampas se queda ~10 min atrás
    assert parse_iso(wm_bueno) >= fin_a > parse_iso(wm_antes)
