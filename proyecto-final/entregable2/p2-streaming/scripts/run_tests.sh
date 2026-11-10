#!/usr/bin/env bash
# Corre las pruebas DENTRO del contenedor de Glue (ver docker_run.sh para lanzarlo desde el host).
# Uso: scripts/run_tests.sh [argumentos de pytest]
set -euo pipefail
cd "$(dirname "$0")/.."
export TZ=UTC PYTHONDONTWRITEBYTECODE=1
exec python3 -m pytest -q -p no:cacheprovider "$@"
