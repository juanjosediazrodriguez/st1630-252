"""Apaga cómputo de demo sin borrar datos ni el stack CloudFormation."""
from __future__ import annotations

from deploy import config


def main() -> None:
    cfg, session = config()
    out = {x["OutputKey"]: x["OutputValue"] for x in session.client("cloudformation").describe_stacks(
        StackName=f"{cfg['PROJECT_PREFIX']}-e2")["Stacks"][0]["Outputs"]}
    glue = session.client("glue")
    for key in ("StreamingJob", "GoldJob"):
        job = out[key]
        runs = glue.get_job_runs(JobName=job, MaxResults=10)["JobRuns"]
        active = [r["Id"] for r in runs if r["JobRunState"] in ("STARTING", "RUNNING")]
        if active:
            glue.batch_stop_job_run(JobName=job, JobRunIds=active)
            print(f"Deteniendo {job}: {active}")
        else:
            print(f"{job}: sin ejecución activa")
    ec2 = session.client("ec2")
    for key in ("AirflowInstanceId", "SupersetInstanceId"):
        if key not in out:
            continue
        instance = out[key]
        state = ec2.describe_instances(InstanceIds=[instance])["Reservations"][0]["Instances"][0]["State"]["Name"]
        if state not in ("stopped", "stopping"):
            ec2.stop_instances(InstanceIds=[instance])
            print(f"Deteniendo EC2 {instance}")
        else:
            print(f"EC2 {instance}: {state}")
    print("Kinesis permanece activo mientras exista el stack; eliminar el stack cuando no se necesite la demo.")


if __name__ == "__main__":
    main()
