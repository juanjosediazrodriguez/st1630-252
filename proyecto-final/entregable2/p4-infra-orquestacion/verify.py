"""Comprobaciones de despliegue sin iniciar jobs de Glue ni enviar tráfico."""
from __future__ import annotations

import argparse
import json
import time

from deploy import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssm", action="store_true", help="Verificar bootstrap de la EC2 mediante SSM")
    parser.add_argument("--debug-ec2", action="store_true", help="Estado de contenedor y cloud-init sin secretos")
    args = parser.parse_args()
    cfg, session = config()
    stack = f"{cfg['PROJECT_PREFIX']}-e2"
    cfn = session.client("cloudformation")
    data = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    out = {x["OutputKey"]: x["OutputValue"] for x in data["Outputs"]}
    print("CloudFormation:", data["StackStatus"])
    print("Kinesis:", session.client("kinesis").describe_stream_summary(StreamName=out["StreamName"])["StreamDescriptionSummary"]["StreamStatus"])
    for key in ("AlertsQueueUrl", "CartsQueueUrl", "AlertsDlqUrl", "CartsDlqUrl"):
        q = session.client("sqs").get_queue_attributes(QueueUrl=out[key], AttributeNames=["ApproximateNumberOfMessages"])
        print(key, q["Attributes"].get("ApproximateNumberOfMessages", "?"))
    for key in ("StreamingJob", "GoldJob"):
        j = session.client("glue").get_job(JobName=out[key])["Job"]
        print(key, j["GlueVersion"], j["Command"]["Name"])
    subs = session.client("sns").list_subscriptions_by_topic(TopicArn=out["AlertsTopicArn"])["Subscriptions"]
    print("SNS:", ", ".join(s["SubscriptionArn"] for s in subs))
    if args.ssm and "AirflowInstanceId" in out:
        instance = out["AirflowInstanceId"]
        commands = [
            "systemctl is-active docker",
            "test -f /opt/tiendacol/dags/tiendacol_dags.py && echo dags-present",
            "docker ps --filter name=tiendacol-airflow --format '{{.Names}} {{.Status}}'",
            "docker exec tiendacol-airflow airflow dags list --output json 2>/dev/null | python3 -c 'import json,sys; print([d[\"dag_id\"] for d in json.load(sys.stdin) if d[\"dag_id\"].startswith(\"tiendacol_\")])'",
        ]
        if args.debug_ec2:
            commands = [
                "docker ps -a --format '{{.Names}} {{.Status}}'",
                "tail -n 80 /var/log/cloud-init-output.log",
            ]
        ssm = session.client("ssm")
        command_id = ssm.send_command(InstanceIds=[instance], DocumentName="AWS-RunShellScript", Parameters={"commands": commands})["Command"]["CommandId"]
        for _ in range(30):
            time.sleep(5)
            result = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance)
            if result["Status"] in ("Success", "Failed", "TimedOut", "Cancelled"):
                print("SSM:", result["Status"])
                print(result.get("StandardOutputContent", "")[-3000:])
                print(result.get("StandardErrorContent", "")[-2000:])
                if result["Status"] != "Success":
                    raise SystemExit(1)
                break
        else:
            raise TimeoutError("SSM no completó la verificación")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
