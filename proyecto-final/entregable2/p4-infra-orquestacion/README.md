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

El script valida CloudFormation, crea el stack `tiendacol-e2`, sube los paquetes Python a S3 y habilita las Lambdas. Crea EC2 `t3.medium` separadas para Airflow y Superset, sin puertos entrantes. No inicia Glue Streaming: se arranca desde el DAG `tiendacol_bootstrap` después de cargar y validar maestros. Para desplegar sin EC2: `--no-airflow`; para omitir Superset: `--no-superset`.

El stack genera los nombres de los buckets y emite salidas para las colas, el stream, los jobs, el workgroup de Athena y la EC2. El correo de SNS debe confirmar la suscripción. La EC2 no abre ningún puerto entrante: el acceso a Airflow se hace con AWS Systems Manager Session Manager, reenviando el puerto local 8080 al puerto 8080 de la instancia. La contraseña inicial de `airflow standalone` queda en los logs del contenedor (`docker logs tiendacol-airflow` desde una sesión SSM). El rol de la instancia entrega credenciales a los DAGs por IMDS; no se copian claves al servidor.

Antes de una sesión posterior, iniciar Airflow y Superset con `python entregable2/p4-infra-orquestacion/start_airflow.py`. Para acceder a Airflow: `python entregable2/p4-infra-orquestacion/open_ui.py airflow` y abrir `http://localhost:8080`; para Superset: `python entregable2/p4-infra-orquestacion/open_ui.py superset` y abrir `http://localhost:8088`. Estos comandos usan las credenciales de `entregable2/.env` y requieren AWS CLI y Session Manager Plugin. La contraseña de Superset queda en `entregable2/.env` tras ejecutar `setup_superset.py`. Para actualizar los DAGs tras cambiar código: `python entregable2/p4-infra-orquestacion/deploy.py --upload-only` y `python entregable2/p4-infra-orquestacion/operate.py sync-dags`. El script `operate.py` también permite disparar bootstrap, Gold y stop desde SSM.

Después del primer despliegue, `python entregable2/p4-infra-orquestacion/setup_superset.py` crea de forma idempotente la conexión Athena, cuatro datasets, cuatro gráficos y el dashboard. Superset usa el rol IAM de su EC2, sin copiar claves AWS al servidor. El dashboard y sus cuatro consultas se verificaron mediante la API de Superset.

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
4. Al terminar, ejecutar `tiendacol_stop_demo`; verificar que Glue Streaming queda `STOPPED`. Luego ejecutar `python entregable2/p4-infra-orquestacion/stop_costs.py` desde el equipo local para detener ambas EC2 y cualquier job que siga activo. Kinesis sigue facturando mientras exista el stream. Cuando ya no se necesite la demo, ejecutar `python entregable2/p4-infra-orquestacion/destroy_demo.py --confirm tiendacol-e2` para eliminar el stack y el stream. Los buckets S3 quedan retenidos por CloudFormation y deben revisarse por separado.

Para la prueba RNF-01, `python entregable2/p4-infra-orquestacion/stress_rnf01.py` escala el stream por etapas hasta seis shards, corre el generador P1 con `--ramp 500,1000,2000,3000,5000`, registra tasas y atraso de lectura, y vuelve por etapas a uno en un bloque `finally`. Kinesis limita cada cambio de shards a un máximo del doble al crecer o la mitad al decrecer; `deploy.py --shards 6` y `deploy.py --shards 1` aplican los pasos necesarios. Los umbrales de CloudWatch incluyen IteratorAge (dos minutos), mensajes en ambas DLQ y eventos de fallo de Glue.

Para verificar RF-07/08/09 en AWS, lanzar los escenarios reales con `run_generator.py scenario` y luego `verify_live.py rf07|rf08|rf09`; RF-08 requiere que el verificador de DynamoDB se inicie apenas se lanza el escenario. Ejecutar RF-09 en un minuto sin otras órdenes: `agg_sales_minute` agrega todas las órdenes del minuto. Para repetir un escenario, usar `--scenario-seed` distinto para evitar reutilizar `event_id`. Los JSON observados quedan en `evidence/aws-scenarios/`.

`python entregable2/p4-infra-orquestacion/test_rf12.py` inyecta una cabecera inválida en el maestro de productos, comprueba el fallo y las dependencias bloqueadas, restaura el objeto S3 en un bloque `finally` y dispara una ejecución de recuperación. `python entregable2/p4-infra-orquestacion/test_scd2.py` sube un segundo snapshot determinista, ejecuta Gold y comprueba versiones históricas en Athena. Ejecutar ambos cuando no haya escenarios de streaming en curso. `python entregable2/p4-infra-orquestacion/stress_rnf01.py` prueba la rampa con seis shards y restablece uno al terminar; tarda varios minutos y genera tráfico real. Antes de detener Airflow, `python entregable2/p4-infra-orquestacion/archive_airflow_logs.py` copia sus logs a S3 ops.

El presupuesto mensual envía avisos al 50 % y 80 % del valor configurado; no es un límite duro. Antes de pruebas largas, revisar el saldo de créditos y el estado de los jobs. No dejar Glue Streaming activo entre sesiones.

Consulta de negocio validada: `python entregable2/p4-infra-orquestacion/query_gold.py`. Con el escenario RF-13 se observaron 100 órdenes, COP 14.561.000 en ventas y ticket promedio COP 145.610. La segunda ejecución de Gold dejó los mismos resultados.

Tras una ejecución Gold actualizada, `python entregable2/p4-infra-orquestacion/verify_gold_aws.py` consulta en Athena SCD2, las etapas del embudo (RF-10), ventas y ticket promedio (RF-11) y el cruce de `event_id` de Gold con Bronze (RNF-06). Escribe `observed_gold.json` en `evidence/aws-scenarios/`. La ejecución Gold de la rampa completa tardó cerca de siete minutos con entrada repartida en grupos de 10.000 registros; a volúmenes mucho mayores convendría migrar las transformaciones pandas del driver a operaciones Spark distribuidas.

## Cifrado

La cuenta nueva devolvió `KMS NotFoundException` durante su aprovisionamiento inicial. Cuando las claves administradas quedaron habilitadas se actualizó el stack: **S3 usa `alias/aws/s3`, Kinesis `alias/aws/kinesis` y DynamoDB SSE-KMS**. SQS usa SSE-SQS, cifrado administrado por el servicio, como alternativa de bajo costo a una clave KMS propia. `deploy.py` comprueba que las claves KMS estén disponibles antes de crear un stack nuevo. Los objetos y eventos escritos antes de habilitar KMS conservan su cifrado original.
