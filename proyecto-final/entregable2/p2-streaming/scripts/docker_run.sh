#!/usr/bin/env bash
# Lanza un comando dentro del contenedor de Glue 4.0 con esta carpeta (y p1-generador) montadas.
# Uso (Git Bash):  scripts/docker_run.sh scripts/run_tests.sh -k parse
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
E2="$(dirname "$HERE")"
export MSYS_NO_PATHCONV=1
WIN_E2="$(cd "$E2" && (pwd -W 2>/dev/null || pwd))"
exec docker run --rm -v "${WIN_E2}:/home/glue_user/workspace/entregable2" \
  -e DISABLE_SSL=true -e DATALAKE_FORMATS=delta -e TZ=UTC \
  --entrypoint bash public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01 \
  -lc "cd /home/glue_user/workspace/entregable2/p2-streaming && $*"
