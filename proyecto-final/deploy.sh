#!/usr/bin/env sh
# Entrada desde la raíz: CloudFormation es el equivalente funcional a Compose para AWS.
set -eu
exec python3 entregable2/p4-infra-orquestacion/deploy.py "$@"
