"""DAGs de control: Glue procesa los datos; esta EC2 solo llama APIs de AWS."""
from __future__ import annotations

import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).parent / "vendor"))

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
LAKE = os.environ["LAKE_BUCKET"]
OPS = os.environ["OPS_BUCKET"]
STREAM = os.environ["STREAM_NAME"]
STREAM_JOB = os.environ["GLUE_STREAMING_JOB"]
GOLD_JOB = os.environ["GLUE_GOLD_JOB"]
TOPIC = os.environ["ALERTS_TOPIC_ARN"]
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "tiendacol-analytics")
BOGOTA = ZoneInfo("America/Bogota")
START = datetime(2026, 11, 1, tzinfo=ZoneInfo("UTC"))


def client(name):
    return boto3.client(name, region_name=REGION)


def on_failure(context):
    task = context["task_instance"]
    client("sns").publish(
        TopicArn=TOPIC,
        Subject=f"TiendaCol: fallo {task.dag_id}"[:100],
        Message=f"DAG={task.dag_id}, task={task.task_id}, run={task.run_id}. Revisar logs de Airflow.",
    )


DEFAULT = {"owner": "DanielArcila", "retries": 0, "on_failure_callback": on_failure}


def load_masters(**_kwargs):
    from tiendacol_gen.masters import build_masters, upload_to_s3, write_masters

    today = datetime.now(BOGOTA).date()
    day = today.isoformat()
    # La primera carga fija la semilla del día 0; los días posteriores generan
    # cambios reales para que Gold pueda registrar historial SCD Tipo 2.
    response = client("s3").list_objects_v2(Bucket=LAKE, Prefix="landing/masters/_manifest_dt=")
    previous = sorted(
        key.split("_manifest_dt=", 1)[1].split(".json", 1)[0]
        for obj in response.get("Contents", [])
        if (key := obj["Key"]).endswith(".json")
    )
    first_day = date.fromisoformat(previous[0]) if previous else today
    day_index = max(0, (today - first_day).days)
    with tempfile.TemporaryDirectory(prefix="tiendacol-masters-") as tmp:
        masters = build_masters(seed=1630, day_index=day_index, n_products=1000, n_customers=2000, fixtures=True)
        manifest = write_masters(masters, Path(tmp), day)
        count = upload_to_s3(Path(tmp), LAKE)
    if count != 5 or not all(n > 0 for n in manifest["counts"].values()):
        raise RuntimeError(f"Maestros incompletos: {manifest}")


def check_masters(**_kwargs):
    import csv
    import io
    import json
    from tiendacol_gen.masters import PRODUCT_FIELDS, CUSTOMER_FIELDS, SELLER_FIELDS, INVENTORY_FIELDS

    s3 = client("s3")
    day = datetime.now(BOGOTA).date().isoformat()
    base = "landing/masters"
    manifest = json.loads(s3.get_object(Bucket=LAKE, Key=f"{base}/_manifest_dt={day}.json")["Body"].read())
    fields = {"products": PRODUCT_FIELDS, "customers": CUSTOMER_FIELDS,
              "sellers": SELLER_FIELDS, "inventory": INVENTORY_FIELDS}
    if set(manifest.get("files", {})) != set(fields):
        raise ValueError(f"Manifiesto incompleto: {list(manifest.get('files', {}))}")
    for name, rel in manifest["files"].items():
        data = s3.get_object(Bucket=LAKE, Key=f"{base}/{rel}")["Body"].read().decode("utf-8")
        if rel.endswith(".csv"):
            reader = csv.DictReader(io.StringIO(data))
            if reader.fieldnames != fields[name]:
                raise ValueError(f"Columnas inválidas de {name}: {reader.fieldnames}")
            rows = list(reader)
            if any(None in row or not row.get(fields[name][0]) for row in rows):
                raise ValueError(f"Filas inválidas de {name}")
        else:
            rows = json.loads(data)
        if len(rows) != manifest["counts"][name]:
            raise ValueError(f"Conteo inválido de {name}: {len(rows)} != {manifest['counts'][name]}")


def start_streaming(**_kwargs):
    glue = client("glue")
    runs = glue.get_job_runs(JobName=STREAM_JOB, MaxResults=5)["JobRuns"]
    if any(r["JobRunState"] in ("STARTING", "RUNNING", "STOPPING") for r in runs):
        return
    glue.start_job_run(JobName=STREAM_JOB)


def check_streaming(**_kwargs):
    runs = client("glue").get_job_runs(JobName=STREAM_JOB, MaxResults=5)["JobRuns"]
    if not runs or runs[0]["JobRunState"] in ("STOPPED", "STOPPING"):
        # Apagado intencional entre sesiones de demo.
        return
    if runs[0]["JobRunState"] not in ("STARTING", "RUNNING"):
        raise RuntimeError(f"Glue Streaming falló: {runs[0]['JobRunState']}")
    client("kinesis").describe_stream_summary(StreamName=STREAM)


def check_silver(**_kwargs):
    response = client("s3").list_objects_v2(Bucket=LAKE, Prefix="silver/events/_delta_log/", MaxKeys=1)
    if response.get("KeyCount", 0) < 1:
        raise RuntimeError("Silver todavía no tiene log Delta")


def run_gold(**_kwargs):
    glue = client("glue")
    run_id = glue.start_job_run(JobName=GOLD_JOB)["JobRunId"]
    deadline = time.monotonic() + 35 * 60
    while time.monotonic() < deadline:
        state = glue.get_job_run(JobName=GOLD_JOB, RunId=run_id)["JobRun"]["JobRunState"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "STOPPED", "TIMEOUT", "ERROR"):
            raise RuntimeError(f"Gold {run_id}: {state}")
        time.sleep(20)
    raise TimeoutError(f"Gold {run_id} no terminó en 35 minutos")


def register_catalog(**_kwargs):
    athena = client("athena")
    base = Path(__file__).parent / "sql"
    for filename in ("athena_ddl.sql", "ddl_gold.sql"):
        statements = []
        for raw in (base / filename).read_text(encoding="utf-8").replace("<LAKE_BUCKET>", LAKE).split(";"):
            sql = "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith("--")).strip()
            # Las bases tiendacol_lake y tiendacol_gold ya existen por CloudFormation.
            if sql and sql.upper().startswith("CREATE EXTERNAL TABLE"):
                statements.append(sql)
        for sql in statements:
            execution = athena.start_query_execution(
                QueryString=sql,
                WorkGroup=WORKGROUP,
                ResultConfiguration={"OutputLocation": f"s3://{OPS}/athena-results/"},
            )["QueryExecutionId"]
            while True:
                status = athena.get_query_execution(QueryExecutionId=execution)["QueryExecution"]["Status"]
                if status["State"] == "SUCCEEDED":
                    break
                if status["State"] in ("FAILED", "CANCELLED"):
                    raise RuntimeError(f"Athena DDL: {status.get('StateChangeReason', status['State'])}")
                time.sleep(2)


def stop_streaming(**_kwargs):
    glue = client("glue")
    runs = glue.get_job_runs(JobName=STREAM_JOB, MaxResults=5)["JobRuns"]
    active = [r["Id"] for r in runs if r["JobRunState"] in ("STARTING", "RUNNING")]
    if active:
        glue.batch_stop_job_run(JobName=STREAM_JOB, JobRunIds=active)


with DAG("tiendacol_bootstrap", start_date=START, schedule=None, catchup=False, default_args=DEFAULT,
         description="Maestros validados antes de iniciar streaming") as bootstrap:
    masters = PythonOperator(task_id="load_masters", python_callable=load_masters)
    valid = PythonOperator(task_id="validate_masters", python_callable=check_masters)
    stream = PythonOperator(task_id="start_streaming", python_callable=start_streaming)
    masters >> valid >> stream

with DAG("tiendacol_masters_daily", start_date=START, schedule="0 10 * * *", catchup=False,
         default_args=DEFAULT) as daily:
    load = PythonOperator(task_id="load_masters", python_callable=load_masters)
    verify = PythonOperator(task_id="validate_masters", python_callable=check_masters)
    load >> verify

with DAG("tiendacol_health", start_date=START, schedule="*/5 * * * *", catchup=False,
         default_args=DEFAULT) as health:
    PythonOperator(task_id="check_streaming", python_callable=check_streaming)

with DAG("tiendacol_gold_hourly", start_date=START, schedule="5 * * * *", catchup=False,
         default_args=DEFAULT) as hourly:
    m = PythonOperator(task_id="validate_masters", python_callable=check_masters)
    s = PythonOperator(task_id="validate_silver", python_callable=check_silver)
    g = PythonOperator(task_id="run_gold", python_callable=run_gold)
    c = PythonOperator(task_id="register_catalog", python_callable=register_catalog)
    m >> s >> g >> c

with DAG("tiendacol_stop_demo", start_date=START, schedule=None, catchup=False,
         default_args=DEFAULT) as stop:
    PythonOperator(task_id="stop_streaming", python_callable=stop_streaming)
