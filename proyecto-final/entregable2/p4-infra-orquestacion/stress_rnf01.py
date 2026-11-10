"""Mide la rampa RNF-01 con 6 shards y restaura 1 shard al terminar."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deploy import HERE, config, outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step-seconds", type=float, default=60,
                        help="segundos por escalón; 60 para medición completa")
    args = parser.parse_args()
    cfg, session = config()
    evidence = HERE / "evidence" / "aws-scenarios"
    evidence.mkdir(parents=True, exist_ok=True)
    stack = f"{cfg['PROJECT_PREFIX']}-e2"
    out = outputs(session.client("cloudformation"), stack)
    report = evidence / "rnf01-ramp.json"
    start = datetime.now(timezone.utc)
    try:
        subprocess.run([sys.executable, str(HERE / "deploy.py"), "--shards", "6"], check=True)
        shards = session.client("kinesis").describe_stream_summary(StreamName=out["StreamName"])["StreamDescriptionSummary"]["OpenShardCount"]
        if shards != 6:
            raise AssertionError(f"Se esperaban 6 shards; AWS reporta {shards}")
        start = datetime.now(timezone.utc)
        subprocess.run([
            sys.executable, str(HERE / "run_generator.py"), "run", "--sink", "kinesis",
            "--stream", out["StreamName"], "--seed", "1630", "--products", "1000",
            "--customers", "2000", "--ramp", "500,1000,2000,3000,5000",
            "--step-seconds", str(args.step_seconds), "--workers", "16", "--report", str(report),
        ], check=True)
        end = datetime.now(timezone.utc)
        # CloudWatch publica métricas con retraso; la espera evita declarar ausencia prematura.
        time.sleep(90)
        cloudwatch = session.client("cloudwatch")
        metric = cloudwatch.get_metric_statistics(
            Namespace="AWS/Kinesis", MetricName="GetRecords.IteratorAgeMilliseconds",
            Dimensions=[{"Name": "StreamName", "Value": out["StreamName"]}],
            StartTime=start - timedelta(minutes=1), EndTime=end + timedelta(minutes=2),
            Period=60, Statistics=["Maximum"],
        )
        points = sorted(({"timestamp": p["Timestamp"].isoformat(), "max_ms": p["Maximum"]}
                         for p in metric.get("Datapoints", [])), key=lambda x: x["timestamp"])
        ramp = json.loads(report.read_text(encoding="utf-8"))
        result = {"start": start.isoformat(), "end": end.isoformat(), "shards": shards,
                  "steps": ramp["steps"], "sink": ramp["sink"], "iterator_age": points,
                  "maximum_measured_events_per_second": max(p["achieved_rate"] for p in ramp["steps"]),
                  "all_steps_sustained": all(p["sustained"] for p in ramp["steps"])}
        (evidence / "observed_rnf01.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2), flush=True)
    finally:
        subprocess.run([sys.executable, str(HERE / "deploy.py"), "--shards", "1"], check=True)
        print("Kinesis restaurado a 1 shard", flush=True)


if __name__ == "__main__":
    main()
