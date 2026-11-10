"""Orquestación del job: tablas -> maestros -> consultas.

En producción (Glue) las cinco consultas corren a la vez en la misma aplicación de Spark. Con `--run_once`
(pruebas) se ejecutan una tras otra con `availableNow`: primero Q1 hasta vaciar la fuente y luego Q2..Q5,
cada una hasta vaciar Silver.
"""
from __future__ import annotations

import importlib
import logging

from .config import Config, apply_runtime_conf, build_spark
from .masters import get_holder
from .tables import ensure_tables

log = logging.getLogger("tiendacol.job")

# nombre de la consulta -> (módulo, función de arranque)
STARTERS = {
    "ingesta": ("ingesta", "start_ingesta"),
    "stock": ("stock", "start_stock"),
    "alertas": ("alertas", "start_alertas"),
    "carritos": ("carritos", "start_carritos"),
    "ventas_minuto": ("ventas_minuto", "start_ventas_minuto"),
}


def _starter(name: str):
    mod, fn = STARTERS[name]
    return getattr(importlib.import_module(f"{__package__}.{mod}"), fn)


def prepare(spark, cfg: Config) -> None:
    """Pasos previos a las consultas. Leer como stream una ruta Delta inexistente falla, por eso las tablas
    se crean vacías primero; el stock se siembra con el inventario del maestro (RF-03)."""
    get_holder(spark, cfg)                # falla con un error claro si no hay maestros
    created = ensure_tables(spark, cfg)
    if created:
        log.info("Tablas creadas: %s", created)
    if "stock" in cfg.queries or "alertas" in cfg.queries:
        from .stock import seed_stock
        seed_stock(spark, cfg)


def run_job(cfg: Config, spark=None) -> None:
    spark = spark or build_spark()
    apply_runtime_conf(spark, cfg)
    prepare(spark, cfg)
    names = [q for q in STARTERS if q in cfg.queries]
    if cfg.run_once:
        for n in names:
            log.info("run_once: arrancando %s", n)
            q = _starter(n)(spark, cfg)
            q.awaitTermination()          # relanza la excepción de la consulta si falló
        return
    queries = [_starter(n)(spark, cfg) for n in names]
    log.info("Consultas activas: %s", [q.name for q in queries])
    spark.streams.awaitAnyTermination()   # si una cae, el job falla y Glue lo reintenta
