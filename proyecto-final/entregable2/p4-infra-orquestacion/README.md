# P4 · Infraestructura y orquestación de TiendaCol E2

Responsable: Daniel Arcila Salazar. Región: `us-east-1`.

## Preparación

1. Copiar `entregable2/.env.example` a `entregable2/.env` y completar credenciales AWS, correos y presupuesto. `.env` está ignorado por Git.
2. Verificar la cuenta: `aws freetier get-account-plan-state`. El plan debe ser `PAID` para usar Kinesis Data Streams. Los créditos restantes siguen aplicándose.
3. Instalar dependencias locales: `python -m pip install boto3 python-dotenv pandas`. El generador P1 usa pandas incluso al iniciar su CLI.

## Despliegue

Desde la raíz `proyecto-final`:

```powershell
python entregable2/p4-infra-orquestacion/deploy.py
```

El script valida CloudFormation, crea el stack `tiendacol-e2`, sube los paquetes Python a S3 y habilita las Lambdas. La última actualización crea una EC2 `t3.medium` para Airflow. No inicia Glue Streaming: se arranca desde el DAG `tiendacol_bootstrap` después de cargar y validar maestros. Para desplegar sin EC2: `--no-airflow`.

El stack genera los nombres de los buckets y emite salidas para las colas, el stream, los jobs, el workgroup de Athena y la EC2. El correo de SNS debe confirmar la suscripción. La EC2 no abre ningún puerto entrante: el acceso a Airflow se hace con AWS Systems Manager Session Manager, reenviando el puerto local 8080 al puerto 8080 de la instancia. La contraseña inicial de `airflow standalone` queda en los logs del contenedor (`docker logs tiendacol-airflow` desde una sesión SSM). El rol de la instancia entrega credenciales a los DAGs por IMDS; no se copian claves al servidor.

Antes de una sesión posterior, iniciar Airflow con `python entregable2/p4-infra-orquestacion/start_airflow.py`. Para acceder a la interfaz, usar la salida `AirflowInstanceId` de `verify.py` y abrir una sesión SSM de reenvío del puerto 8080; después visitar `http://localhost:8080` en el navegador. Para actualizar los DAGs tras cambiar código: `python entregable2/p4-infra-orquestacion/deploy.py --upload-only` y `python entregable2/p4-infra-orquestacion/operate.py sync-dags`. El script `operate.py` también permite disparar bootstrap, Gold y stop desde SSM.

## DAGs

| DAG | Función |
|---|---|
| `tiendacol_bootstrap` | Genera maestros, valida conteos y arranca Glue Streaming. |
| `tiendacol_masters_daily` | Renueva maestros diariamente a las 05:00 hora de Colombia. |
| `tiendacol_health` | Comprueba cada cinco minutos que Glue Streaming esté activo. |
| `tiendacol_gold_hourly` | Valida maestros y Silver, ejecuta Glue Batch y registra tablas Delta en Athena. |
| `tiendacol_stop_demo` | Detiene Glue Streaming. |

Los DAGs usan `boto3`; la EC2 es plano de control y no procesa Spark. Un fallo en `validate_masters` o `validate_silver` bloquea los pasos dependientes y dispara una notificación SNS. Para RF-12, corregir los datos y limpiar/reintentar el DAG en Airflow.

## Demo y costos

1. Ejecutar `tiendacol_bootstrap` y esperar que Glue Streaming marque `RUNNING`.
2. Lanzar P1 con `run_generator.py run --sink kinesis --stream tiendacol-events --seed 1630 --products 1000 --customers 2000`; este envoltorio carga `.env` sin imprimir credenciales. Los valores deben coincidir con los maestros del DAG.
3. Consultar Bronze/Silver y ejecutar `tiendacol_gold_hourly`. Usar el workgroup `tiendacol-analytics` para consultas de Gold.
4. Al terminar, ejecutar `tiendacol_stop_demo`; verificar que Glue Streaming queda `STOPPED`. Luego ejecutar `python entregable2/p4-infra-orquestacion/stop_costs.py` desde el equipo local para detener también la EC2 y cualquier job que siga activo. Kinesis sigue facturando mientras exista el stream; eliminar el stack efímero cuando termine la evaluación, conservando los buckets del lago si se requiere evidencia.

El presupuesto mensual envía avisos al 50 % y 80 % del valor configurado; no es un límite duro. Antes de pruebas largas, revisar el saldo de créditos y el estado de los jobs. No dejar Glue Streaming activo entre sesiones.

Consulta de negocio validada: `python entregable2/p4-infra-orquestacion/query_gold.py`. Con el escenario RF-13 se observaron 100 órdenes, COP 14.561.000 en ventas y ticket promedio COP 145.610. La segunda ejecución de Gold dejó los mismos resultados.

## Cifrado

La cuenta nueva devolvió `KMS NotFoundException` durante su aprovisionamiento inicial. Cuando las claves administradas quedaron habilitadas se actualizó el stack: **S3 usa `alias/aws/s3`, Kinesis `alias/aws/kinesis` y DynamoDB SSE-KMS**. SQS usa SSE-SQS, cifrado administrado por el servicio, como alternativa de bajo costo a una clave KMS propia. `deploy.py` comprueba que las claves KMS estén disponibles antes de crear un stack nuevo. Los objetos y eventos escritos antes de habilitar KMS conservan su cifrado original.
