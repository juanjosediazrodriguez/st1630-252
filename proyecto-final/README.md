# TiendaCol en tiempo real · Entregable 2

La implementación y las instrucciones principales están en [entregable2/README.md](entregable2/README.md).

## Infraestructura reproducible

El equivalente funcional al `docker-compose.yml` de la raíz para el pipeline completo es la plantilla
[CloudFormation](entregable2/p4-infra-orquestacion/cloudformation/tiendacol.yaml). Despliega Kinesis,
Glue Streaming y Batch, el lago Delta en S3, SQS/Lambda/SNS/DynamoDB, Athena, Airflow y Superset.
Glue y Kinesis son servicios administrados de AWS, por lo que el pipeline completo se ejecuta en
la cuenta AWS. [El Compose de Superset](entregable2/p1-generador/superset/docker-compose.yml) permite
usar el tablero en un equipo local con Docker.
Los puntos de entrada de la raíz son [`deploy.ps1`](deploy.ps1) en PowerShell y
[`deploy.sh`](deploy.sh) en Linux/macOS.

```powershell
Copy-Item entregable2/.env.example entregable2/.env
# Completar .env sin subirlo a Git.
python -m pip install boto3 python-dotenv pandas
python entregable2/p4-infra-orquestacion/deploy.py
python entregable2/p4-infra-orquestacion/verify.py
python entregable2/p4-infra-orquestacion/start_airflow.py
python entregable2/p4-infra-orquestacion/operate.py trigger-bootstrap
```

El [manual de P4](entregable2/p4-infra-orquestacion/README.md) contiene los comandos para generar
tráfico, consultar Gold, usar Airflow y Superset, ejecutar pruebas y retirar la infraestructura.
La [bitácora consolidada](docs/bitacora_ia.md) registra el uso de agentes de IA.
El [estado de evidencia](docs/entregable2/estado_evidencia.md) resume las pruebas ejecutadas en AWS.
