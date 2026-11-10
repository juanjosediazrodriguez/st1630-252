"""Abre un túnel SSM local hacia Airflow o Superset (Ctrl+C para cerrar)."""
from __future__ import annotations

import argparse
import json
import os
import subprocess

from deploy import config, outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=("airflow", "superset"))
    args = parser.parse_args()
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    key, port = (("AirflowInstanceId", "8080") if args.service == "airflow"
                 else ("SupersetInstanceId", "8088"))
    env = os.environ.copy()
    env.pop("AWS_PROFILE", None)
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        if cfg.get(name):
            env[name] = cfg[name]
        else:
            env.pop(name, None)
    if cfg.get("AWS_PROFILE") and not cfg.get("AWS_ACCESS_KEY_ID"):
        env["AWS_PROFILE"] = cfg["AWS_PROFILE"]
    env["AWS_DEFAULT_REGION"] = cfg["AWS_REGION"]
    print(f"Abrir http://localhost:{port} mientras esta terminal siga activa.", flush=True)
    subprocess.run([
        "aws", "ssm", "start-session", "--target", out[key],
        "--document-name", "AWS-StartPortForwardingSession",
        "--parameters", json.dumps({"portNumber": [port], "localPortNumber": [port]}),
        "--region", cfg["AWS_REGION"],
    ], env=env, check=True)


if __name__ == "__main__":
    main()
