"""Retira los recursos facturables de la demo; conserva los buckets S3 del lago y ops."""
from __future__ import annotations

import argparse

from deploy import config, outputs


def main() -> None:
    cfg, session = config()
    stack = f"{cfg['PROJECT_PREFIX']}-e2"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", required=True, help=f"Escribir exactamente {stack}")
    args = parser.parse_args()
    if args.confirm != stack:
        parser.error(f"--confirm debe ser {stack}")

    cfn = session.client("cloudformation")
    out = outputs(cfn, stack)
    glue = session.client("glue")
    for key in ("StreamingJob", "GoldJob"):
        runs = glue.get_job_runs(JobName=out[key], MaxResults=10)["JobRuns"]
        active = [r["Id"] for r in runs if r["JobRunState"] in ("STARTING", "RUNNING")]
        if active:
            glue.batch_stop_job_run(JobName=out[key], JobRunIds=active)
            raise RuntimeError(f"Se solicitó detener {out[key]}; esperar STOPPED y repetir")

    cfn.delete_stack(StackName=stack)
    cfn.get_waiter("stack_delete_complete").wait(StackName=stack)
    print(f"Stack {stack} eliminado. Kinesis, EC2, Glue y demás recursos efímeros retirados.")
    print("Buckets conservados por DeletionPolicy Retain:")
    print(f"  lake: {out['LakeBucket']}")
    print(f"  ops:  {out['OpsBucket']}")
    print("Los buckets y sus objetos versionados pueden seguir generando cargos; revisar su ciclo de vida.")


if __name__ == "__main__":
    main()
