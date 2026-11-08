"""CLI del generador: python -m tiendacol_gen <masters|run|scenario|verify> ..."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from . import scenarios, verify
from .events import STREAM_TYPES, EventFactory
from .masters import build_masters, upload_to_s3, write_masters
from .runner import results_to_dicts, run_ramp
from .sinks import FileSink, KinesisSink, NullSink


def _masters(a):
    return build_masters(seed=a.seed, day_index=a.day_index, n_products=a.products, n_customers=a.customers,
                         fixtures=not a.no_fixtures)


def _stream_map(a) -> dict[str, str]:
    if a.stream_map:
        m = dict(kv.split("=", 1) for kv in a.stream_map.split(","))
        missing = set(STREAM_TYPES) - set(m)
        if missing:
            sys.exit(f"--stream-map debe cubrir {sorted(missing)}")
        return m
    return {t: a.stream for t in STREAM_TYPES}


def _sink(a):
    if a.sink == "kinesis":
        return KinesisSink(_stream_map(a), region=a.region, workers=a.workers)
    if a.sink == "file":
        return FileSink(a.out)
    return NullSink()


def cmd_masters(a):
    day = a.date or date.today().isoformat()
    m = _masters(a)
    manifest = write_masters(m, Path(a.out), day, products_format=a.products_format)
    print(json.dumps(manifest, indent=2))
    if a.bucket:
        n = upload_to_s3(Path(a.out), a.bucket, a.prefix)
        print(f"subidos {n} archivos a s3://{a.bucket}/{a.prefix}/")


def cmd_run(a):
    f = EventFactory(_masters(a), a.seed, dup_rate=a.dup_rate, late_rate=a.late_rate, late_seconds=a.late_seconds,
                     malformed_rate=a.malformed_rate, active_sessions=a.active_sessions, hot_products=a.hot_products)
    sink = _sink(a)
    rates = [int(x) for x in a.ramp.split(",")] if a.ramp else [a.rate]
    secs = a.step_seconds if a.ramp else a.duration
    results = run_ramp(f, sink, rates, secs, on_step=lambda r: print(
        f"objetivo {r.target_rate:>5} ev/s -> logrado {r.achieved_rate:>7.1f} ev/s en {r.seconds}s {'OK' if r.sustained else 'NO SOSTENIDO'}"))
    closing = sink.close()
    st = f.stats
    report = {"steps": results_to_dicts(results), "stats": {"emitidos": st.emitted, "logicos": st.logical, "duplicados": st.duplicates,
                                                            "tardios": st.late, "malformados": st.malformed, "por_stream": st.by_stream},
              "sink": closing}
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if a.report:
        Path(a.report).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def cmd_scenario(a):
    if a.name == "list":
        print("\n".join(scenarios.SCENARIOS))
        return
    sc = scenarios.SCENARIOS[a.name](_masters(a))
    path = scenarios.write_expected(sc, a.expected_dir)
    print(f"escenario {sc.name}: {len(sc.items)} registros · esperado en {path}")
    if sc.realtime and not a.fast:
        last = max(i.at for i in sc.items)
        print(f"se reproduce en tiempo real (~{last / 60:.1f} min). No cierres la terminal.")
    sink = _sink(a)
    n = scenarios.play(sc, sink, realtime=False if a.fast else None)
    print(json.dumps({"enviados": n, "sink": sink.close()}, default=str))


def cmd_verify(a):
    exp = json.loads(Path(a.expected).read_text(encoding="utf-8"))["expected"]
    L = verify.load_table
    name = Path(a.expected).stem.removeprefix("expected_")
    if name == "rf01":
        r = verify.verify_rf01(exp, L(a.bronze))
    elif name == "rf02":
        r = verify.verify_rf02(exp, L(a.bronze))
    elif name == "rf03":
        r = verify.verify_rf03(exp, L(a.table))
    elif name == "rf05":
        r = verify.verify_rf05(exp, L(a.silver), L(a.quarantine))
    elif name == "rnf03":
        r = verify.verify_rnf03(exp, L(a.bronze), L(a.silver))
    elif name == "rf07":
        r = verify.verify_rf07(exp, [json.loads(x) for x in Path(a.alerts).read_text(encoding="utf-8").splitlines() if x])
    elif name == "rf09":
        r = verify.verify_rf09(exp, L(a.table))
    elif name == "rf13":
        r = verify.verify_rf13(exp, L(a.table))
    else:
        sys.exit(f"verify no soporta '{name}' desde la CLI (RF-08 usa watch_dynamodb_carts; ver README)")
    print(r)
    sys.exit(0 if r.ok else 1)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tiendacol_gen", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--seed", type=int, default=42)
        sp.add_argument("--day-index", type=int, default=0, help="día N de deriva de los maestros")
        sp.add_argument("--products", type=int, default=2000)
        sp.add_argument("--customers", type=int, default=20000)
        sp.add_argument("--no-fixtures", action="store_true", help="sin productos/clientes TEST-*")

    def sink_args(sp):
        sp.add_argument("--sink", choices=["kinesis", "file", "null"], default="file")
        sp.add_argument("--stream", default="tiendacol-events", help="un único stream para los tres tipos")
        sp.add_argument("--stream-map", help="clickstream=a,orders=b,stock_changes=c (anula --stream)")
        sp.add_argument("--region", default="us-east-1")
        sp.add_argument("--workers", type=int, default=16)
        sp.add_argument("--out", default="out/events", help="directorio para --sink file")

    m = sub.add_parser("masters", help="genera maestros diarios (RF-04)")
    common(m)
    m.add_argument("--out", default="out/masters")
    m.add_argument("--date", help="AAAA-MM-DD del snapshot (por defecto hoy)")
    m.add_argument("--products-format", choices=["csv", "json"], default="csv")
    m.add_argument("--bucket", help="si se indica, sube a s3://BUCKET/PREFIX/")
    m.add_argument("--prefix", default="landing/masters")
    m.set_defaults(fn=cmd_masters)

    r = sub.add_parser("run", help="emite tráfico continuo o una rampa (RNF-01)")
    common(r)
    sink_args(r)
    r.add_argument("--rate", type=int, default=100, help="eventos/s")
    r.add_argument("--duration", type=float, default=60)
    r.add_argument("--ramp", help="lista de tasas, p. ej. 500,1000,2000,3000,5000")
    r.add_argument("--step-seconds", type=float, default=60)
    r.add_argument("--dup-rate", type=float, default=0.01)
    r.add_argument("--late-rate", type=float, default=0.01)
    r.add_argument("--late-seconds", type=float, default=120)
    r.add_argument("--malformed-rate", type=float, default=0.01)
    r.add_argument("--active-sessions", type=int, default=2000)
    r.add_argument("--hot-products", type=int, default=0, help="N productos concentran el 50 %% de la demanda")
    r.add_argument("--report", help="guarda el reporte JSON")
    r.set_defaults(fn=cmd_run)

    s = sub.add_parser("scenario", help="reproduce un escenario de prueba (o 'list')")
    common(s)
    sink_args(s)
    s.add_argument("name", choices=["list", *scenarios.SCENARIOS])
    s.add_argument("--expected-dir", default="out/expected")
    s.add_argument("--fast", action="store_true", help="no esperar tiempo real (invalida RF-07/08/09)")
    s.set_defaults(fn=cmd_scenario)

    v = sub.add_parser("verify", help="compara lo esperado contra los datos observados")
    v.add_argument("expected", help="out/expected/expected_<escenario>.json")
    v.add_argument("--bronze"), v.add_argument("--silver"), v.add_argument("--quarantine")
    v.add_argument("--table", help="tabla observada (Gold/stock/ventas por minuto/top)")
    v.add_argument("--alerts", help="JSONL con {product_id, received_at}")
    v.set_defaults(fn=cmd_verify)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
