# Ambiente local del Lab 2b

Ejecuta Spark en Ubuntu WSL con Java 17. El entorno de PowerShell sirve
para AWS CLI y el productor, pero Spark nativo en Windows requiere
binarios adicionales de Hadoop que no estan instalados.

Desde PowerShell, en la raiz del repositorio:

```powershell
wsl -d Ubuntu
```

Dentro de Ubuntu, conserva esta terminal abierta mientras trabajas:

```bash
cd /mnt/c/Users/User/st1630-252
source labs/lab2b-kinesis/activate.sh
sudo docker compose -f labs/lab2a-kafka/docker-compose.yml up -d --wait
sudo docker exec st1630-lab2a-kafka kafka-topics --create --if-not-exists --topic pedidos-ventas --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
python labs/lab2b-kinesis/scripts/check_environment.py
```

Kafka UI: http://localhost:8080. Broker: localhost:9092.

Los tres TODO obligatorios de `scripts/streaming_pipeline.py` estan
completos. Ejecuta el pipeline desde el mismo entorno activado:

```bash
python labs/lab2b-kinesis/scripts/streaming_pipeline.py
```

Publica datos antes de iniciar el pipeline:

```bash
python labs/lab2a-kafka/efcortesr/scripts/productor_kafka.py
```

Para comprobar Spark, Delta, el conector Kafka y el resultado generado:

```bash
python labs/lab2b-kinesis/scripts/check_environment.py
```

La ejecucion verificada produjo 6 agregados para 1.000 pedidos y cero
claves duplicadas por `(window_start, window_end, region)`.

Las versiones de Spark 3.5.6 y Delta 3.3.3 son compatibles con Scala 2.12
usado por el script. Referencia: https://docs.delta.io/releases/

## AWS Academy

Perfil local: `lab2b`. Region: `us-east-1`. Credenciales temporales en
`.aws/credentials`, excluidas de Git. La activacion configura la ruta;
no modifica tus perfiles globales. Renueva las tres claves de ese archivo
cuando expire la sesion de Academy.

STS y `kinesis list-streams` se verificaron correctamente el 2026-09-13.
`kinesis create-stream` fallo con `AccessDeniedException`: el rol `voclabs`
no tiene `kinesis:CreateStream`. No se creo ningun stream. Solicita al
responsable de AWS Academy habilitar ese permiso para la Parte 4.

Ademas, el ejemplo `.format("aws-kinesis")` usa opciones del conector AWS,
pero el README indica descargar el conector Qubole. Esa combinacion no
esta validada y debe corregirse antes de ejecutar la Parte 4.
Referencia: https://github.com/awslabs/spark-sql-kinesis-connector

Para usar solamente AWS CLI o el productor desde PowerShell:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
. .\labs\lab2b-kinesis\activate.ps1
aws sts get-caller-identity
aws kinesis list-streams --region us-east-1
```

Para detener Kafka sin eliminar sus datos:

```bash
sudo docker compose -f labs/lab2a-kafka/docker-compose.yml stop
```
