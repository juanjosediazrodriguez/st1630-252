"""Despliegue reproducible de TiendaCol E2 desde entregable2/.env.

Etapas: CloudFormation base -> subir artefactos -> Lambdas -> Airflow EC2.
No inicia Glue Streaming: se inicia con el DAG bootstrap tras cargar maestros.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, WaiterError
from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent
E2 = HERE.parent
TEMPLATE = HERE / "cloudformation" / "tiendacol.yaml"


def config() -> tuple[dict, boto3.Session]:
    values = {k: v for k, v in dotenv_values(E2 / ".env").items() if v}
    required = ("AWS_REGION", "PROJECT_PREFIX", "ALERT_EMAIL", "BUDGET_EMAIL", "MONTHLY_BUDGET_USD")
    missing = [name for name in required if not values.get(name)]
    if missing:
        raise SystemExit(f"Faltan en entregable2/.env: {', '.join(missing)}")
    if values.get("AWS_ACCESS_KEY_ID") and values.get("AWS_SECRET_ACCESS_KEY"):
        session = boto3.Session(
            aws_access_key_id=values["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=values["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=values.get("AWS_SESSION_TOKEN"),
            region_name=values["AWS_REGION"],
        )
    else:
        session = boto3.Session(profile_name=values.get("AWS_PROFILE"), region_name=values["AWS_REGION"])
    return values, session


def parameters(cfg: dict, apps: bool, airflow: bool, vpc: str = "", subnet: str = "") -> list[dict]:
    values = {
        "Prefix": cfg["PROJECT_PREFIX"],
        "AlertEmail": cfg["ALERT_EMAIL"],
        "BudgetEmail": cfg["BUDGET_EMAIL"],
        "MonthlyBudgetUsd": cfg["MONTHLY_BUDGET_USD"],
        "EnableApplications": str(apps).lower(),
        "EnableAirflow": str(airflow).lower(),
        "AirflowVpcId": vpc,
        "AirflowSubnetId": subnet,
    }
    return [{"ParameterKey": k, "ParameterValue": str(v)} for k, v in values.items()]


def outputs(cfn, stack: str) -> dict[str, str]:
    data = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    return {x["OutputKey"]: x["OutputValue"] for x in data.get("Outputs", [])}


def apply_stack(cfn, stack: str, params: list[dict]) -> dict[str, str]:
    template = TEMPLATE.read_text(encoding="utf-8")
    cfn.validate_template(TemplateBody=template)
    try:
        cfn.describe_stacks(StackName=stack)
        existing = True
    except ClientError as exc:
        if "does not exist" not in str(exc):
            raise
        existing = False
    action = "update" if existing else "create"
    print(f"CloudFormation {action}: {stack}", flush=True)
    arguments = dict(
        StackName=stack,
        TemplateBody=template,
        Parameters=params,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
        Tags=[{"Key": "Project", "Value": "TiendaCol"}, {"Key": "Owner", "Value": "DanielArcila"}],
    )
    try:
        if existing:
            cfn.update_stack(**arguments)
        else:
            cfn.create_stack(**arguments, OnFailure="DO_NOTHING")
    except ClientError as exc:
        if "No updates are to be performed" in str(exc):
            return outputs(cfn, stack)
        raise
    waiter = cfn.get_waiter("stack_update_complete" if existing else "stack_create_complete")
    try:
        waiter.wait(StackName=stack, WaiterConfig={"Delay": 15, "MaxAttempts": 80})
    except WaiterError:
        events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
        failures = [e for e in events if "FAILED" in e["ResourceStatus"]]
        for event in failures[:8]:
            print(f"{event['LogicalResourceId']}: {event.get('ResourceStatusReason', '')}", file=sys.stderr)
        raise
    return outputs(cfn, stack)


def zip_bytes(files: list[tuple[Path, str]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for local, key in files:
            archive.write(local, key)
    return buffer.getvalue()


def package_dir(directory: Path, package: str) -> bytes:
    files = [(p, p.relative_to(directory.parent).as_posix()) for p in directory.rglob("*.py")]
    if not files:
        raise FileNotFoundError(directory)
    return zip_bytes(files)


def upload_artifacts(s3, out: dict) -> None:
    ops = out["OpsBucket"]
    artifacts = {
        "glue/tiendacol_stream.zip": package_dir(E2 / "p2-streaming" / "tiendacol_stream", "tiendacol_stream"),
        "glue/tiendacol_gold.zip": package_dir(E2 / "p3-gold-batch" / "tiendacol_gold", "tiendacol_gold"),
        "lambda/consumers.zip": zip_bytes([(HERE / "lambda" / "handlers.py", "handlers.py")]),
    }
    dag_files = [(HERE / "airflow" / "tiendacol_dags.py", "tiendacol_dags.py")]
    dag_files += [(p, (Path("vendor") / "tiendacol_gen" / p.relative_to(E2 / "p1-generador" / "tiendacol_gen")).as_posix())
                  for p in (E2 / "p1-generador" / "tiendacol_gen").rglob("*.py")]
    dag_files += [
        (E2 / "p2-streaming" / "sql" / "athena_ddl.sql", "sql/athena_ddl.sql"),
        (E2 / "p3-gold-batch" / "sql" / "ddl_gold.sql", "sql/ddl_gold.sql"),
    ]
    artifacts["airflow/dags.zip"] = zip_bytes(dag_files)
    for key, body in artifacts.items():
        s3.put_object(Bucket=ops, Key=key, Body=body)
        print(f"Subido s3://{ops}/{key} ({len(body)} bytes)")
    scripts = {
        "glue/tiendacol_streaming.py": E2 / "p2-streaming" / "glue" / "tiendacol_streaming.py",
        "glue/tiendacol_gold_batch.py": E2 / "p3-gold-batch" / "glue" / "tiendacol_gold_batch.py",
    }
    for key, path in scripts.items():
        s3.upload_file(str(path), ops, key)
        print(f"Subido s3://{ops}/{key}")


def default_network(ec2) -> tuple[str, str]:
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise RuntimeError("No hay VPC predeterminada para la EC2 de Airflow")
    vpc = vpcs[0]["VpcId"]
    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc]}])["Subnets"]
    public = [s for s in subnets if s["MapPublicIpOnLaunch"]]
    if not public:
        raise RuntimeError("No hay subnet pública en la VPC predeterminada")
    return vpc, sorted(public, key=lambda s: s["SubnetId"])[0]["SubnetId"]


def wait_for_kms(session) -> None:
    """Cuentas nuevas pueden tardar en aprovisionar las claves AWS administradas."""
    kms = session.client("kms")
    aliases = ("alias/aws/s3", "alias/aws/kinesis", "alias/aws/dynamodb")
    for attempt in range(18):
        try:
            states = [kms.describe_key(KeyId=alias)["KeyMetadata"]["KeyState"] for alias in aliases]
            if all(state == "Enabled" for state in states):
                return
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("NotFoundException", "KMSInvalidStateException"):
                raise
        if attempt == 0:
            print("Esperando claves KMS administradas por AWS en la cuenta nueva...", flush=True)
        time.sleep(10)
    raise TimeoutError("KMS no está listo; intentar de nuevo antes de crear el stack")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-airflow", action="store_true", help="Crear recursos administrados y Lambdas sin EC2")
    parser.add_argument("--upload-only", action="store_true", help="Actualizar paquetes en S3 sin modificar CloudFormation")
    args = parser.parse_args()
    cfg, session = config()
    identity = session.client("sts").get_caller_identity()
    plan = session.client("freetier").get_account_plan_state()["accountPlanType"]
    if plan != "PAID":
        raise RuntimeError(f"El plan de la cuenta es {plan}; Kinesis requiere PAID")
    print(f"Cuenta: {identity['Account']}; región: {cfg['AWS_REGION']}; plan: {plan}")
    wait_for_kms(session)
    cfn = session.client("cloudformation")
    stack = f"{cfg['PROJECT_PREFIX']}-e2"
    if args.upload_only:
        existing = outputs(cfn, stack)
        upload_artifacts(session.client("s3"), existing)
        return
    try:
        current = outputs(cfn, stack)
    except ClientError as exc:
        if "does not exist" not in str(exc):
            raise
        current = None
    if current is not None:
        upload_artifacts(session.client("s3"), current)
        if args.no_airflow:
            vpc, subnet = "", ""
        else:
            vpc, subnet = default_network(session.client("ec2"))
        final = apply_stack(cfn, stack, parameters(cfg, True, not args.no_airflow, vpc, subnet))
        print(json.dumps(final, indent=2))
        return
    base = apply_stack(cfn, stack, parameters(cfg, False, False))
    upload_artifacts(session.client("s3"), base)
    apps = apply_stack(cfn, stack, parameters(cfg, True, False))
    if args.no_airflow:
        final = apps
    else:
        vpc, subnet = default_network(session.client("ec2"))
        final = apply_stack(cfn, stack, parameters(cfg, True, True, vpc, subnet))
    print(json.dumps(final, indent=2))
    print("Glue Streaming no está iniciado. Cargar maestros y lanzar tiendacol_bootstrap en Airflow.")


if __name__ == "__main__":
    main()
