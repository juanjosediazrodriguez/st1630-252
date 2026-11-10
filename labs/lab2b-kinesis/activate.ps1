$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$pythonExe = Join-Path $repoRoot '.venv/Scripts/python.exe'
if (-not (Test-Path $pythonExe)) {
    throw 'Falta instalar el entorno .venv del laboratorio.'
}
. (Join-Path $repoRoot '.venv/Scripts/Activate.ps1')
$env:AWS_SHARED_CREDENTIALS_FILE = Join-Path $repoRoot '.aws/credentials'
$env:AWS_PROFILE = 'lab2b'
$env:AWS_DEFAULT_REGION = 'us-east-1'
$env:AWS_REGION = 'us-east-1'
$env:KINESIS_REGION = 'us-east-1'
$env:KINESIS_STREAM = 'pedidos-ventas-kinesis'
$env:STREAM_SOURCE = 'kafka'
$env:PYSPARK_PYTHON = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:SILVER_STREAMING_PATH = Join-Path $repoRoot '.lab2b/lake/silver/ventas_streaming'
$env:CHECKPOINT_PATH = Join-Path $repoRoot '.lab2b/lake/checkpoints/lab2b-kafka'
$localJava = Join-Path $repoRoot '.lab2b/java'
if (Test-Path $localJava) {
    $env:JAVA_HOME = $localJava
    $env:PATH = "$localJava/bin;$env:PATH"
} elseif (Test-Path 'C:/Program Files/Java/jre-1.8/bin/java.exe') {
    $env:JAVA_HOME = 'C:/Program Files/Java/jre-1.8'
    $env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
}
Write-Host 'Lab2b: entorno activado, region us-east-1, fuente Kafka.'
