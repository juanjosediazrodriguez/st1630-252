"""Configura el dashboard de Superset en EC2 mediante SSM, sin publicar puertos."""
from __future__ import annotations

import re
import time
from pathlib import Path

from deploy import E2, config, outputs


def run_ssm(session, instance: str, commands: list[str]) -> str:
    ssm = session.client("ssm")
    command_id = ssm.send_command(
        InstanceIds=[instance], DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
    )["Command"]["CommandId"]
    for _ in range(90):
        time.sleep(3)
        result = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
        if result["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
            if result["Status"] != "Success":
                raise RuntimeError(result.get("StandardErrorContent", "")[-1000:] or result["Status"])
            return result.get("StandardOutputContent", "")
    raise TimeoutError("SSM no respondió")


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    instance = out["SupersetInstanceId"]
    uri = (f"awsathena+rest://@athena.{cfg['AWS_REGION']}.amazonaws.com:443/tiendacol_gold"
           f"?s3_staging_dir=s3://{out['OpsBucket']}/athena-results/&work_group={out['AthenaWorkGroup']}")
    commands = [
        f"aws s3 cp s3://{out['OpsBucket']}/superset/bootstrap_superset.py /tmp/bootstrap_superset.py --region {cfg['AWS_REGION']}",
        "docker cp /tmp/bootstrap_superset.py tiendacol-superset:/tmp/bootstrap_superset.py",
        f"docker exec -e ATHENA_URI='{uri}' tiendacol-superset python /tmp/bootstrap_superset.py",
        "curl -fsS http://127.0.0.1:8088/health >/dev/null",
    ]
    print(run_ssm(session, instance, commands)[-1500:])

    # Se guarda en el .env ignorado por Git para que Daniel pueda entrar al tablero.
    password = run_ssm(session, instance, ["cat /opt/tiendacol/superset/admin-password"]).strip()
    if not re.fullmatch(r"[0-9a-f]{36}", password):
        raise RuntimeError("La contraseña generada tiene un formato inesperado")
    path = E2 / ".env"
    content = path.read_text(encoding="utf-8")
    if re.search(r"(?m)^SUPERSET_ADMIN_PASSWORD=", content):
        content = re.sub(r"(?m)^SUPERSET_ADMIN_PASSWORD=.*$", f"SUPERSET_ADMIN_PASSWORD={password}", content)
    else:
        content += f"\nSUPERSET_ADMIN_PASSWORD={password}\n"
    path.write_text(content, encoding="utf-8")
    print("Contraseña de Superset guardada en entregable2/.env (ignorado por Git).")


if __name__ == "__main__":
    main()
