"""Aplica los verificadores P1 de RF-01/02/03/05 y RNF-03 a datos AWS reales."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from deploy import E2, config, outputs
from verify_live import athena_rows

sys.path.insert(0, str(E2 / "p1-generador"))
from tiendacol_gen import verify as p1  # noqa: E402


EVIDENCE = Path(__file__).parent / "evidence" / "aws-scenarios"
SCENARIO_SEEDS = {"rf01": 1001, "rf02": 1002, "rf03": 1003, "rf05": 1005, "rnf03": 1006}


def records(session, out, table, columns, t0):
    since = t0[:19].replace("T", " ")
    return athena_rows(session, out, f"SELECT {columns} FROM tiendacol_lake.{table} "
                       f"WHERE ingested_at >= TIMESTAMP '{since}'")


def await_count(session, out, table, t0, minimum, timeout_s=900):
    deadline = time.monotonic() + timeout_s
    last = 0
    while time.monotonic() < deadline:
        raw = records(session, out, table, "COUNT(*)", t0)
        last = int(raw[0][0])
        if last >= minimum:
            return last
        time.sleep(20)
    raise TimeoutError(f"{table}: {last}/{minimum} registros tras {t0}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("rf01", "rf02", "rf03", "rf05", "rnf03"))
    args = parser.parse_args()
    source = EVIDENCE / f"expected_{args.scenario}.json"
    doc = json.loads(source.read_text(encoding="utf-8"))
    expected, t0 = doc["expected"], doc["t0"]
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")

    if args.scenario in ("rf01", "rf02"):
        await_count(session, out, "bronze_events", t0,
                    expected["count"] if args.scenario == "rf01" else len(expected["orders"]) + len(expected["status_change_event_ids"]))
        raw = records(session, out, "bronze_events", "raw", t0)
        events = []
        for row in raw:
            try:
                events.append(json.loads(row[0]))
            except (ValueError, TypeError):
                pass
        frame = pd.DataFrame(events)
        result = (p1.verify_rf01(expected, frame) if args.scenario == "rf01"
                  else p1.verify_rf02(expected, frame))
        observed = {"bronze_rows_since_start": len(raw)}
    elif args.scenario == "rf03":
        await_count(session, out, "bronze_events", t0, len(expected["event_ids"]))
        ids = list(expected["stock_by_product"])
        quoted = ",".join("'" + x.replace("'", "''") + "'" for x in ids)
        data = athena_rows(session, out, f"SELECT product_id, SUM(stock) FROM tiendacol_lake.stock_current "
                           f"WHERE product_id IN ({quoted}) GROUP BY product_id")
        frame = pd.DataFrame([{"product_id": x[0], "stock": int(x[1])} for x in data])
        result = p1.verify_rf03(expected, frame)
        observed = {"stock": frame.to_dict("records")}
    elif args.scenario == "rf05":
        await_count(session, out, "bronze_events", t0, expected["sent"])
        await_count(session, out, "silver_events", t0, expected["silver_unique"])
        ids = set(expected["silver_event_ids"])
        raw = records(session, out, "silver_events", "event_id", t0)
        mine = [r[0] for r in raw if r[0] in ids]
        quarantine = records(session, out, "quarantine_events", "event_id", t0)
        result = p1.verify_rf05(expected, pd.DataFrame({"event_id": mine}),
                                pd.DataFrame({"event_id": [r[0] for r in quarantine]}))
        observed = {"silver_expected_ids_found": len(mine), "quarantine_rows": len(quarantine)}
    else:
        await_count(session, out, "bronze_events", t0, expected["bronze_count"])
        await_count(session, out, "silver_events", t0, expected["silver_unique"])
        ids = set(expected["events"])
        bronze = [r[0] for r in records(session, out, "bronze_events", "event_id", t0) if r[0] in ids]
        silver = [r[0] for r in records(session, out, "silver_events", "event_id", t0) if r[0] in ids]
        result = p1.verify_rnf03(expected, pd.DataFrame({"event_id": bronze}),
                                 pd.DataFrame({"event_id": silver}))
        observed = {"bronze_expected_rows": len(bronze), "silver_expected_rows": len(silver)}

    evidence = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": result.ok,
                "result": str(result), "scenario_t0": t0,
                "scenario_seed": SCENARIO_SEEDS[args.scenario],
                "masters_day_index": 0 if args.scenario == "rf03" else 1,
                **observed}
    path = EVIDENCE / f"observed_{args.scenario}.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(evidence, indent=2, ensure_ascii=False, default=str), flush=True)
    if not result.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
