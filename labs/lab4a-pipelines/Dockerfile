# Dockerfile — Lab 4a (ST1630-2026-2, S11)
#
# Extiende la imagen oficial de Airflow 2.9 para poder correr PySpark +
# Delta Lake DENTRO de los contenedores del scheduler/webserver -- la
# imagen oficial no trae JDK, y PySpark lo necesita para levantar la JVM.
#
# Combinación de versiones (Java 17 + pyspark 3.5.1 + delta-spark 3.1.0):
# es la misma que ya quedó probada funcionando en este curso para correr
# Spark + Delta Lake en local (fuera de un clúster EMR) -- no la cambies
# sin probarla tú mismo primero.
FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    # La ruta real de instalación de Java depende de la arquitectura de
    # tu máquina (amd64 en Intel/AMD, arm64 en Apple Silicon) -- en vez
    # de hardcodear una, creamos un symlink estable que apunta a donde
    # sea que apt lo haya puesto de verdad.
    && ln -sfn "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.1.0
