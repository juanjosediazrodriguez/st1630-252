"""Check Spark, Delta and Kafka connector without running the lab exercises."""
from pathlib import Path
import tempfile
import os

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

root = Path(__file__).resolve().parents[3]
builder = (
    SparkSession.builder.master("local[2]")
    .appName("lab2b-environment-check")
    .config("spark.jars.ivy", str(root / ".lab2b" / "ivy-linux"))
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.databricks.delta.snapshotPartitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"],
).getOrCreate()
try:
    assert spark.range(5).count() == 5
    with tempfile.TemporaryDirectory(prefix="lab2b-check-") as directory:
        spark.range(5).write.format("delta").save(directory + "/delta")
        assert spark.read.format("delta").load(directory + "/delta").count() == 5
    print(f"Spark {pyspark.__version__}: OK; Delta read/write OK; Kafka JAR loaded")

    result_path = os.environ.get("SILVER_STREAMING_PATH")
    if result_path and Path(result_path).exists():
        result = spark.read.format("delta").load(result_path)
        rows = result.count()
        orders = result.agg({"num_pedidos": "sum"}).first()[0]
        duplicate_keys = rows - result.select(
            "window_start", "window_end", "region"
        ).distinct().count()
        print(f"Resultado: {rows} filas; {orders} pedidos; {duplicate_keys} claves duplicadas")
finally:
    spark.stop()
