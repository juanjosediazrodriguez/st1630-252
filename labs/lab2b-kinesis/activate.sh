#!/usr/bin/env bash
LAB2B_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$LAB2B_ROOT/.venv-linux/bin/activate"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export AWS_SHARED_CREDENTIALS_FILE="$LAB2B_ROOT/.aws/credentials"
export AWS_PROFILE=lab2b
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export KINESIS_REGION=us-east-1
export KINESIS_STREAM=pedidos-ventas-kinesis
export STREAM_SOURCE=kafka
export PYSPARK_PYTHON="$LAB2B_ROOT/.venv-linux/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
export SILVER_STREAMING_PATH="$LAB2B_ROOT/.lab2b/lake/silver/ventas_streaming"
export CHECKPOINT_PATH="$LAB2B_ROOT/.lab2b/lake/checkpoints/lab2b-kafka"
export PYSPARK_SUBMIT_ARGS="--master local[2] --conf spark.jars.ivy=$LAB2B_ROOT/.lab2b/ivy-linux --conf spark.sql.shuffle.partitions=4 --conf spark.databricks.delta.snapshotPartitions=4 pyspark-shell"
echo 'Lab2b activado en WSL; region us-east-1; fuente Kafka.'
