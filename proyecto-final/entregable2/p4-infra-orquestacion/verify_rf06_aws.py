"""Contrasta una muestra de Silver en Athena con el snapshot de maestros usado."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from deploy import E2, config, outputs
from verify_live import athena_rows

sys.path.insert(0, str(E2 / "p1-generador"))
from tiendacol_gen.masters import build_masters  # noqa: E402
from tiendacol_gen.verify import verify_rf06  # noqa: E402


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    manifests = session.client("s3").list_objects_v2(
        Bucket=out["LakeBucket"], Prefix="landing/masters/_manifest_dt=")["Contents"]
    first_day = min(x["Key"].split("_manifest_dt=", 1)[1].split(".json", 1)[0]
                    for x in manifests if x["Key"].endswith(".json"))
    sql = ("SELECT event_id, product_id, category_id, seller_id, user_id, city, department "
           f"FROM tiendacol_lake.silver_events WHERE masters_dt = '{first_day}' LIMIT 1000")
    raw = athena_rows(session, out, sql)
    columns = ("event_id", "product_id", "category_id", "seller_id", "user_id", "city", "department")
    frame = pd.DataFrame(raw, columns=columns)
    frame["category_id"] = pd.to_numeric(frame["category_id"], errors="coerce")
    masters = build_masters(seed=1630, day_index=0, n_products=1000, n_customers=2000, fixtures=True)
    result = verify_rf06(masters, frame, n=1000)
    data = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": result.ok,
            "result": str(result), "masters_day": first_day, "sample_size": len(frame)}
    path = Path(__file__).parent / "evidence" / "aws-scenarios" / "observed_rf06.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    if not result.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
