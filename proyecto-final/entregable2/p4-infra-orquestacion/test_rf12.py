"""Inyecta y recupera un fallo de maestros en el DAG Gold; restaura siempre S3."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from deploy import config, outputs
from setup_superset import run_ssm


DAG = "tiendacol_gold_hourly"


def task_states(session, instance, run_id):
    output = run_ssm(session, instance, [
        f"docker exec tiendacol-airflow airflow tasks states-for-dag-run {DAG} {run_id} --output json"
    ])
    return {task["task_id"]: task["state"] for task in json.loads(output)}


def await_states(session, instance, run_id, wanted, timeout_s):
    deadline = time.monotonic() + timeout_s
    latest = {}
    while time.monotonic() < deadline:
        latest = task_states(session, instance, run_id)
        if latest.get("validate_masters") == wanted:
            return latest
        time.sleep(10)
    raise TimeoutError(f"{run_id}: validate_masters no llegó a {wanted}; último estado={latest}")


def trigger(session, instance, run_id):
    run_ssm(session, instance, [
        f"docker exec tiendacol-airflow airflow dags trigger {DAG} -r {run_id} --output json"
    ])


def main():
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    s3 = session.client("s3")
    day = datetime.now(ZoneInfo("America/Bogota")).date().isoformat()
    manifest = json.loads(s3.get_object(Bucket=out["LakeBucket"], Key=f"landing/masters/_manifest_dt={day}.json")["Body"].read())
    key = f"landing/masters/{manifest['files']['products']}"
    original = s3.get_object(Bucket=out["LakeBucket"], Key=key)
    body, version = original["Body"].read(), original["VersionId"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    failed_run, recovered_run = f"rf12-fail-{stamp}", f"rf12-recovered-{stamp}"
    instance = out["AirflowInstanceId"]
    failure = {}
    try:
        s3.put_object(Bucket=out["LakeBucket"], Key=key, Body=b"invalid_column\nx\n")
        trigger(session, instance, failed_run)
        failure = await_states(session, instance, failed_run, "failed", 180)
        for _ in range(12):
            if all(failure.get(t) == "upstream_failed" for t in ("validate_silver", "run_gold", "register_catalog")):
                break
            time.sleep(5)
            failure = task_states(session, instance, failed_run)
        if not all(failure.get(t) == "upstream_failed" for t in ("validate_silver", "run_gold", "register_catalog")):
            raise AssertionError(f"Las dependencias no quedaron bloqueadas: {failure}")
        print(f"Fallo controlado: {failure}", flush=True)
    finally:
        s3.put_object(Bucket=out["LakeBucket"], Key=key, Body=body)
        print(f"Maestro restaurado desde la versión {version}", flush=True)

    trigger(session, instance, recovered_run)
    deadline = time.monotonic() + 1200
    recovery = {}
    while time.monotonic() < deadline:
        recovery = task_states(session, instance, recovered_run)
        if recovery.get("register_catalog") == "success":
            break
        if any(state == "failed" for state in recovery.values()):
            raise AssertionError(f"La recuperación falló: {recovery}")
        time.sleep(15)
    if not recovery or not all(state == "success" for state in recovery.values()):
        raise TimeoutError(f"La recuperación no terminó: {recovery}")
    evidence = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": True,
                "failed_run": failed_run, "failure_states": failure,
                "recovered_run": recovered_run, "recovery_states": recovery,
                "master_key": key, "original_version": version}
    path = Path(__file__).parent / "evidence" / "aws-scenarios" / "observed_rf12.json"
    path.write_text(json.dumps(evidence, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(evidence, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
