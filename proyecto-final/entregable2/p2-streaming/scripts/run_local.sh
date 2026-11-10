#!/usr/bin/env bash
# Corre el job completo en local (dentro del contenedor de Glue), leyendo archivos JSONL en vez de Kinesis.
# Uso: scripts/run_local.sh <carpeta_de_trabajo> <carpeta_con_jsonl> [argumentos extra del job]
#   La carpeta de trabajo debe contener lake/landing/masters (salida de `python -m tiendacol_gen masters`).
set -euo pipefail
cd "$(dirname "$0")/.."
export TZ=UTC PYTHONDONTWRITEBYTECODE=1
WORK="${1:?carpeta de trabajo}"; INPUT="${2:?carpeta con JSONL}"; shift 2
exec spark-submit --master "local[2]" --driver-memory 3g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalogImplementation=in-memory \
  --py-files tiendacol_stream.zip \
  glue/tiendacol_streaming.py \
  --lake_root "$WORK/lake" --checkpoint_root "$WORK/ck" --source files --input_path "$INPUT" \
  --sqs_mode file --decisions_dir "$WORK/decisions" --run_once true --shuffle_partitions 4 \
  --max_files_per_trigger 1 --delta_max_files_per_trigger 1 "$@"
