# Dockerfile — Lab 3 (ST1630-2026-2)
#
# Contenedor de trabajo: Python 3.11 + los drivers de los 4 motores.
# Python 3.11 a propósito: cassandra-driver usa asyncore, que se eliminó
# en Python 3.12.
FROM python:3.11-slim

WORKDIR /lab
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
