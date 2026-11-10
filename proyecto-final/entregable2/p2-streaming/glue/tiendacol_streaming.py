"""Punto de entrada del job de AWS Glue Streaming (también corre en local con spark-submit o python).

Se empaqueta `tiendacol_stream/` en un .zip y se pasa con `--extra-py-files` (ver infra/glue_job.md).
Glue agrega argumentos propios (`--JOB_ID`, `--JOB_RUN_ID`, ...): `parse_known_args` los ignora.
"""
import logging
import sys

from tiendacol_stream.config import apply_runtime_conf, build_spark, parse_config
from tiendacol_stream.job import run_job


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = parse_config(argv if argv is not None else sys.argv[1:])
    spark = build_spark("tiendacol-streaming")
    apply_runtime_conf(spark, cfg)
    run_job(cfg, spark)


if __name__ == "__main__":
    main()
