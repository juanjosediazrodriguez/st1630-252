"""Reinicia solo la EC2 de Airflow antes de una sesión de demo."""
from __future__ import annotations

from deploy import config


def main() -> None:
    cfg, session = config()
    out = {x["OutputKey"]: x["OutputValue"] for x in session.client("cloudformation").describe_stacks(
        StackName=f"{cfg['PROJECT_PREFIX']}-e2")["Stacks"][0]["Outputs"]}
    ec2 = session.client("ec2")
    for key in ("AirflowInstanceId", "SupersetInstanceId"):
        if key not in out:
            continue
        instance = out[key]
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state == "stopped":
            ec2.start_instances(InstanceIds=[instance])
            ec2.get_waiter("instance_running").wait(InstanceIds=[instance])
            print(f"{key} {instance}: running")
        elif state == "running":
            print(f"{key} {instance}: already running")
        else:
            print(f"{key} {instance}: {state}; reintentar cuando termine la transición")


if __name__ == "__main__":
    main()
