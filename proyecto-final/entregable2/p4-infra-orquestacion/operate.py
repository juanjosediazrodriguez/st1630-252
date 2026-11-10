"""Operaciones de demo a través de SSM, sin SSH ni claves en la EC2."""
from __future__ import annotations

import argparse
import time

from deploy import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["trigger-bootstrap", "trigger-gold", "trigger-stop", "list-runs", "stop-runs", "logs", "diagnose", "unpause", "gold-logs", "latest-gold-error", "sync-dags", "catalog-error", "register-only"])
    args = parser.parse_args()
    cfg, session = config()
    outputs = session.client("cloudformation").describe_stacks(StackName=f"{cfg['PROJECT_PREFIX']}-e2")["Stacks"][0]["Outputs"]
    out = {x["OutputKey"]: x["OutputValue"] for x in outputs}
    instance = out["AirflowInstanceId"]
    command = {
        "trigger-bootstrap": "docker exec tiendacol-airflow airflow dags trigger tiendacol_bootstrap",
        "trigger-gold": "docker exec tiendacol-airflow airflow dags trigger tiendacol_gold_hourly",
        "trigger-stop": "docker exec tiendacol-airflow airflow dags trigger tiendacol_stop_demo",
        "list-runs": "docker exec tiendacol-airflow airflow dags list-runs -d tiendacol_bootstrap --output table; docker exec tiendacol-airflow airflow dags list-runs -d tiendacol_gold_hourly --output table",
        "stop-runs": "docker exec tiendacol-airflow airflow dags list-runs -d tiendacol_stop_demo --output table",
        "logs": "docker exec tiendacol-airflow airflow tasks list tiendacol_bootstrap --tree",
        "diagnose": "docker ps -a --filter name=tiendacol-airflow --format '{{.Names}} {{.Status}}'; docker exec tiendacol-airflow airflow jobs check --job-type SchedulerJob; docker exec tiendacol-airflow airflow dags list-import-errors",
        "unpause": "for dag in tiendacol_bootstrap tiendacol_gold_hourly tiendacol_health tiendacol_masters_daily tiendacol_stop_demo; do docker exec tiendacol-airflow airflow dags unpause $dag; done",
        "gold-logs": "find /opt/tiendacol/logs -path '*tiendacol_gold_hourly*' -type f -name '*.log' -printf '%T@ %p\\n' | sort -nr | head -n 4 | cut -d' ' -f2- | while read f; do echo $f; tail -n 18 \"$f\"; done",
        "latest-gold-error": "find /opt/tiendacol/logs -path '*tiendacol_gold_hourly*run_gold*' -type f -name '*.log' -printf '%T@ %p\\n' | sort -nr | head -n 1 | cut -d' ' -f2- | while read f; do echo $f; tail -n 45 \"$f\"; done",
        "sync-dags": f"aws s3 cp s3://{out['OpsBucket']}/airflow/dags.zip /opt/tiendacol/dags.zip --region {cfg['AWS_REGION']} && unzip -o /opt/tiendacol/dags.zip -d /opt/tiendacol/dags >/dev/null && echo dags-synced",
        "catalog-error": "find /opt/tiendacol/logs -path '*tiendacol_gold_hourly*register_catalog*' -type f -name '*.log' -printf '%T@ %p\\n' | sort -nr | head -n 1 | cut -d' ' -f2- | while read f; do echo $f; tail -n 55 \"$f\"; done",
        "register-only": "docker exec tiendacol-airflow python -c 'import sys; sys.path.insert(0, \"/opt/airflow/dags\"); from tiendacol_dags import register_catalog; register_catalog()'",
    }[args.action]
    ssm = session.client("ssm")
    cid = ssm.send_command(InstanceIds=[instance], DocumentName="AWS-RunShellScript", Parameters={"commands": [command]})["Command"]["CommandId"]
    for _ in range(30):
        time.sleep(3)
        result = ssm.get_command_invocation(CommandId=cid, InstanceId=instance)
        if result["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
            print(result["Status"])
            print(result.get("StandardOutputContent", "")[-5000:])
            print(result.get("StandardErrorContent", "")[-2000:])
            if result["Status"] != "Success":
                raise SystemExit(1)
            return
    raise TimeoutError("La operación SSM no terminó")


if __name__ == "__main__":
    main()
