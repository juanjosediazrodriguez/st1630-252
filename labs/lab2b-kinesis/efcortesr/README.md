# Lab 2b - Spark Structured Streaming

**Estudiantes:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio y Mariana Sanchez  
**Fecha de ejecucion:** 2026-09-13  
**Fuente obligatoria:** Kafka  
**Parte opcional Kinesis:** no realizada; AWS Academy nego kinesis:CreateStream

## Configuracion

La ejecucion local usa Ubuntu WSL, Java 17, Python 3.12, Spark 3.5.6 y
Delta Lake 3.3.3. Kafka corre en Docker con cuatro particiones para el
topic pedidos-ventas.

Desde PowerShell:

```powershell
wsl -d Ubuntu
```

Dentro de WSL, desde la raiz del repositorio:

```bash
source labs/lab2b-kinesis/activate.sh
sudo docker compose -f labs/lab2a-kafka/docker-compose.yml up -d --wait
sudo docker exec st1630-lab2a-kafka kafka-topics --create --if-not-exists \
  --topic pedidos-ventas --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

## Ejecucion

```bash
python labs/lab2a-kafka/efcortesr/scripts/productor_kafka.py
python labs/lab2b-kinesis/efcortesr/scripts/streaming_pipeline.py
```

El pipeline lee el JSON desde Kafka, agrega ventas y cantidad de pedidos
por region en ventanas, y escribe los resultados mediante
foreachBatch y MERGE en Delta Lake.

## Resultado verificado

- Kafka y Kafka UI quedaron activos; el broker reporto estado saludable.
- El topic tiene 4 particiones, todas con lider e ISR.
- El productor publico 1.000 pedidos.
- Tras tres publicaciones, la tabla Delta contiene 12 filas para 3.000 pedidos.
- Dos publicaciones cayeron en la misma ventana: el segundo batch actualizo
  sus 6 filas existentes sin crear nuevas claves.
- La verificacion encontro 0 claves duplicadas por
  (window_start, window_end, region).
- Spark, Delta y el conector de Kafka cargaron correctamente.

La evidencia tecnica:

```bash
python labs/lab2b-kinesis/scripts/check_environment.py
```

Las respuestas y decisiones del equipo estan en streaming_design.md.
La declaracion de uso de IA esta en bitacora_delegacion.md.
