"""test_ventas_pipeline.py — Lab 4a (ST1630-2026-2, S11)

Tests de los 3 checks de calidad de datos de ventas_pipeline.py
(check_nulos_criticos, check_rango_monto). Este archivo está
COMPLETO y dado -- es tu autoevaluación objetiva: si implementaste
bien los TODO 1 (nulos críticos) y TODO 2 (rango de monto) en
../ventas_pipeline.py, estos tests deben pasar sin que toques nada
aquí. Si fallan, el problema está en tu implementación, no en el test.

check_cardinalidad_canal no tiene test porque no lanza excepción (es
un logging.warning dado, no un TODO tuyo) -- no hay nada que verificar
con pytest.raises ahí.

Cómo correr estos tests (desde DENTRO del contenedor de Airflow, que ya
trae pyspark/delta instalados -- no hace falta instalar nada más):

    docker compose exec airflow-scheduler pytest /opt/airflow/dags/tests/ -v

Si prefieres correrlos desde tu máquina directamente (fuera de Docker),
necesitas tener instalado localmente: apache-airflow, pyspark==3.5.1,
delta-spark==3.1.0 y Java 17 -- en ese caso:

    cd labs/lab4a-pipelines
    pytest dags/tests/ -v
"""

import os
import sys

# ventas_pipeline.py vive un directorio arriba de este archivo
# (dags/ventas_pipeline.py, este test está en dags/tests/) -- lo
# agregamos a sys.path para poder importarlo como si fuera un módulo
# cualquiera, sin necesitar un paquete instalado.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from pyspark.sql import SparkSession

from ventas_pipeline import check_nulos_criticos, check_rango_monto


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("lab4a-tests")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _df_base(spark, filas):
    """Helper: construye un DataFrame con el esquema mínimo que
    necesitan los checks (pedido_id, producto, total)."""
    return spark.createDataFrame(filas, schema=["pedido_id", "producto", "total"])


def test_validate_no_nulls_passes(spark):
    """DataFrame limpio (sin nulos en las columnas críticas) -> no debe
    lanzar ninguna excepción."""
    df = _df_base(
        spark,
        [
            ("PED-000001", "Audífonos Bluetooth", 89900.0),
            ("PED-000002", "Licuadora 5 velocidades", 129900.0),
            ("PED-000003", "Tenis running", 219900.0),
            ("PED-000004", "Camiseta algodón", 49900.0),
            ("PED-000005", "Arroz 5kg", 24900.0),
        ],
    )
    check_nulos_criticos(df)  # no debe lanzar


def test_validate_nulls_fails(spark):
    """10 filas, 1 con pedido_id nulo = 10% de nulos en esa columna --
    supera el umbral de 5% (UMBRAL_NULOS en ventas_pipeline.py) -> debe
    lanzar ValueError."""
    filas = [(f"PED-{i:06d}", "Producto X", 10000.0) for i in range(9)]
    filas.append((None, "Producto X", 10000.0))  # fila 10: pedido_id nulo
    df = _df_base(spark, filas)

    with pytest.raises(ValueError):
        check_nulos_criticos(df)


def test_validate_negative_totals_fails(spark):
    """Una sola fila con total negativo ya debe ser suficiente para
    que check_rango_monto lance ValueError -- no hay umbral de
    tolerancia para montos inválidos, a diferencia de los nulos."""
    df = _df_base(
        spark,
        [
            ("PED-000001", "Audífonos Bluetooth", 89900.0),
            ("PED-000002", "Licuadora 5 velocidades", -129900.0),
        ],
    )

    with pytest.raises(ValueError):
        check_rango_monto(df)
