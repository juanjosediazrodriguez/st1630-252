"""Sube un segundo snapshot de maestros y verifica historial SCD2 en Athena."""
from __future__ import annotations

import json
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from deploy import E2, config, outputs
from test_rf12 import task_states, trigger
from verify_live import athena_rows

sys.path.insert(0, str(E2 / "p1-generador"))
from tiendacol_gen.masters import build_masters, upload_to_s3, write_masters  # noqa: E402


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    s3 = session.client("s3")
    manifests = s3.list_objects_v2(Bucket=out["LakeBucket"], Prefix="landing/masters/_manifest_dt=")
    days = sorted(date.fromisoformat(x["Key"].split("_manifest_dt=", 1)[1].split(".json", 1)[0])
                  for x in manifests.get("Contents", []) if x["Key"].endswith(".json"))
    if not days:
        raise RuntimeError("No hay snapshot inicial de maestros")
    day1 = (days[0] + timedelta(days=1)).isoformat()
    with tempfile.TemporaryDirectory(prefix="tiendacol-scd2-") as tmp:
        masters = build_masters(seed=1630, day_index=1, n_products=1000, n_customers=2000, fixtures=True)
        write_masters(masters, Path(tmp), day1)
        count = upload_to_s3(Path(tmp), out["LakeBucket"], client=s3)
    if count != 5:
        raise AssertionError(f"Snapshot incompleto: {count} archivos")

    run_id = f"scd2-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    trigger(session, out["AirflowInstanceId"], run_id)
    deadline = time.monotonic() + 1200
    states = {}
    while time.monotonic() < deadline:
        states = task_states(session, out["AirflowInstanceId"], run_id)
        if states.get("register_catalog") == "success":
            break
        if any(state == "failed" for state in states.values()):
            raise AssertionError(f"Gold falló con dos snapshots: {states}")
        time.sleep(20)
    if not states or not all(state == "success" for state in states.values()):
        raise TimeoutError(f"Gold no terminó: {states}")

    rows = athena_rows(session, out, """
        SELECT COUNT(*) AS products_with_history
        FROM (SELECT product_id FROM tiendacol_gold.dim_product
              GROUP BY product_id HAVING COUNT(*) > 1)
    """)
    history = int(rows[0][0])
    sample = athena_rows(session, out, """
        SELECT product_id, COUNT(*) AS versions,
               SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions
        FROM tiendacol_gold.dim_product GROUP BY product_id
        HAVING COUNT(*) > 1 ORDER BY versions DESC LIMIT 5
    """)
    if history < 1 or any(int(r[2]) != 1 for r in sample):
        raise AssertionError(f"Historial SCD2 inválido: {history}, {sample}")
    evidence = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": True,
                "snapshot_day": day1, "files_uploaded": count, "gold_run": run_id,
                "gold_tasks": states, "products_with_history": history, "sample": sample}
    path = Path(__file__).parent / "evidence" / "aws-scenarios" / "observed_scd2.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(evidence, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
