"""Bronze: land raw NDJSON lines untouched in Delta, one partition per export date."""

import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from claims_pipeline.config import BRONZE_DIR, LANDING_DIR, table_path
from claims_pipeline.spark import table_exists

logger = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    pass


def bronze_table(resource_type: str) -> str:
    return table_path(BRONZE_DIR, resource_type.lower())


def _manifest_counts(landing_dir: Path, export_dates: list[str], resource_type: str) -> int:
    total = 0
    for export_date in export_dates:
        manifest = landing_dir / f"export_date={export_date}" / "manifest.json"
        if manifest.exists():
            outputs = json.loads(manifest.read_text(encoding="utf-8"))["output"]
            total += sum(o["count"] for o in outputs if o["type"] == resource_type)
    return total


def ingest(
    spark: SparkSession,
    resource_type: str,
    export_dates: list[str],
    run_id: str,
    landing_dir: Path = LANDING_DIR,
) -> dict:
    files = [
        (landing_dir / f"export_date={d}" / f"{resource_type}.ndjson").as_posix()
        for d in export_dates
        if (landing_dir / f"export_date={d}" / f"{resource_type}.ndjson").exists()
    ]
    expected = _manifest_counts(landing_dir, export_dates, resource_type)
    if not files:
        logger.info("No %s files for %s", resource_type, export_dates)
        return {"resource_type": resource_type, "rows": 0, "expected": expected}

    df = (
        spark.read.text(files)
        .select(
            F.col("value").alias("raw"),
            F.lit(resource_type).alias("resource_type"),
            F.col("_metadata.file_path").alias("source_file"),
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"), r"export_date=(\d{4}-\d{2}-\d{2})", 1
                )
            ).alias("export_date"),
            F.sha2(F.col("value"), 256).alias("record_hash"),
            F.lit(run_id).alias("ingest_run_id"),
            F.current_timestamp().alias("ingested_at"),
        )
        .where(F.length("raw") > 0)
    )

    path = bronze_table(resource_type)
    writer = df.write.format("delta").partitionBy("export_date")
    if table_exists(spark, path):
        predicate = "export_date IN ({})".format(", ".join(f"DATE'{d}'" for d in export_dates))
        writer.mode("overwrite").option("replaceWhere", predicate).save(path)
    else:
        writer.save(path)

    rows = df.count()
    if rows != expected:
        raise ReconciliationError(
            f"{resource_type}: {rows} rows landed in bronze but manifests declare {expected}"
        )
    logger.info("Bronze %s: %s rows reconciled with manifests", resource_type, rows)
    return {"resource_type": resource_type, "rows": rows, "expected": expected}
