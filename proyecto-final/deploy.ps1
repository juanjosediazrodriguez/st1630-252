# Entrada desde la raíz: CloudFormation es el equivalente funcional a Compose para AWS.
param(
    [ValidateRange(1, 6)] [int] $Shards = 1,
    [switch] $NoAirflow,
    [switch] $NoSuperset
)
$deployArgs = @('entregable2/p4-infra-orquestacion/deploy.py', '--shards', [string]$Shards)
if ($NoAirflow) { $deployArgs += '--no-airflow' }
if ($NoSuperset) { $deployArgs += '--no-superset' }
python @deployArgs
exit $LASTEXITCODE
