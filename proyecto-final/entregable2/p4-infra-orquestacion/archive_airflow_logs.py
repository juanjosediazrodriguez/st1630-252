"""Copia logs de tareas Airflow a S3 ops antes de detener la EC2."""
from __future__ import annotations

from deploy import config, outputs
from setup_superset import run_ssm


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    command = (f"aws s3 sync /opt/tiendacol/logs/ s3://{out['OpsBucket']}/airflow-logs/ "
               f"--region {cfg['AWS_REGION']} --only-show-errors && echo airflow-logs-archived")
    result = run_ssm(session, out["AirflowInstanceId"], [command])
    if "airflow-logs-archived" not in result:
        raise RuntimeError(result[-500:])
    print(f"Logs copiados a s3://{out['OpsBucket']}/airflow-logs/")


if __name__ == "__main__":
    main()
