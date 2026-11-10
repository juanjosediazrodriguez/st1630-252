"""Enriquecimiento con maestros (RF-06) y construcción de las filas de Silver.

Los maestros son chicos (miles de filas): *broadcast join*. El evento (la tabla grande, en movimiento)
no se mueve entre ejecutores; la tabla pequeña viaja a cada uno una sola vez por micro-batch.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .parse import silver_select


def enrich_lines(df: DataFrame, products: DataFrame) -> DataFrame:
    """Reemplaza `lines_raw` por `lines`: cada línea con categoría, vendedor y `line_value`.

    Se explotan las líneas, se une cada una con `products` (broadcast) y se vuelven a juntar por `event_id`
    conservando el orden original (`pos`). Requiere `event_id` único en `df` (se deduplica antes).
    `line_value = quantity x unit_price x (1 - discount)` (DISENO §3.3).
    """
    p = products.select(F.col("product_id").alias("p_id"), "category_id", "seller_id")
    exploded = (df.where(F.col("event_type") == "order_created")
                  .select("event_id", F.posexplode("lines_raw").alias("pos", "l")))
    joined = exploded.join(F.broadcast(p), F.col("l.product_id") == F.col("p_id"), "left")
    line = F.struct(
        F.col("l.product_id").alias("product_id"), F.col("l.quantity").alias("quantity"),
        F.col("l.unit_price").alias("unit_price"), F.col("l.discount").alias("discount"),
        F.col("category_id").alias("category_id"), F.col("seller_id").alias("seller_id"),
        (F.col("l.quantity") * F.col("l.unit_price") * (F.lit(1.0) - F.col("l.discount"))).alias("line_value"))
    regrouped = (joined.select("event_id", F.struct(F.col("pos"), line.alias("line")).alias("pl"))
                       .groupBy("event_id").agg(F.sort_array(F.collect_list("pl")).alias("sorted"))
                       .select("event_id", F.transform("sorted", lambda x: x["line"]).alias("lines")))
    return df.join(regrouped, "event_id", "left").drop("lines_raw")


def enrich(typed: DataFrame, products: DataFrame, customers: DataFrame, masters_dt: str) -> DataFrame:
    """Eventos tipados (ver `parse.typed_events`) -> filas de Silver con enriquecimiento.

    - `city`/`department`: del maestro si el usuario está identificado y existe en `customers`
      (`geo_source = 'customer_master'`); si no, del evento (`geo_source = 'event'`).
    - `category_id`, `category`, `seller_id`, `product_name`, `list_price`: de `products` por `product_id`.
    """
    p = products.select(F.col("product_id").alias("p_id"), "product_name", "category_id", "category",
                        "seller_id", "list_price")
    c = customers.select(F.col("customer_id").alias("c_id"), F.col("city").alias("c_city"),
                         F.col("department").alias("c_department"))
    df = (typed.join(F.broadcast(p), typed["product_id"] == F.col("p_id"), "left")
               .join(F.broadcast(c), typed["user_id"] == F.col("c_id"), "left"))
    from_master = F.col("is_identified") & F.col("c_city").isNotNull()
    df = (df.withColumn("city", F.when(from_master, F.col("c_city")).otherwise(F.col("event_city")))
            .withColumn("department", F.when(from_master, F.col("c_department")).otherwise(F.col("event_department")))
            .withColumn("geo_source", F.when(from_master, "customer_master").otherwise("event"))
            .withColumn("masters_dt", F.lit(masters_dt))
            .withColumn("reason", F.col("reason_stock")))
    df = enrich_lines(df, products)
    return silver_select(df)
