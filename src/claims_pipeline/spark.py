import os
from collections.abc import Iterator
from contextlib import contextmanager

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def build_spark(app_name: str = "claims-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.enabled", os.getenv("SPARK_UI_ENABLED", "false"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def spark_session(app_name: str = "claims-pipeline") -> Iterator[SparkSession]:
    spark = build_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()


def table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def upsert(spark: SparkSession, df: DataFrame, path: str, keys: list[str]) -> None:
    """SCD1 upsert: newer `last_updated` wins, re-running the same batch is a no-op."""
    if not table_exists(spark, path):
        df.write.format("delta").save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll(condition="s.last_updated >= t.last_updated")
        .whenNotMatchedInsertAll()
        .execute()
    )
